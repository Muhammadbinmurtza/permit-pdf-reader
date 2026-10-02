"""The contractor on a City of Los Angeles permit, read out of the permit itself.

The City publishes 1,588,259 permits and names a contractor on none of them. LADBS's PRISM portal
publishes each permit as a PDF, and a permit issued through the City's e-permit system carries the
contractor in a text layer: name, licence class, licence number, address and phone. No OCR. Older
permits are scans and yield nothing, so the run goes newest parcel first and records which years
still pay, rather than deciding a cut-off in advance.

PRISM refuses most networks with a 403 from an Azure Front Door rule. It answers GitHub Actions
runners, and it answers ordinary cloud hosts. This script does not care which it runs on.

Resumable and shardable: every parcel already in the output is skipped, and --shard i/n splits the
work so several copies can run side by side without covering the same ground.
"""
import argparse, csv, json, os, queue, re, subprocess, sys, threading, time
import urllib.error, urllib.parse, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE = 'https://prism.dbs.lacity.gov'
SEARCH = BASE + '/api/parcel/search'
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/125.0 Safari/537.36'),
}
# Only documents that record who did the work. The rest of what PRISM holds — affidavits,
# disaster inspection files, administrative approvals — names nobody.
WANTED_DOC_TYPES = {
    'BUILDING PERMIT', 'ELECTRICAL PERMIT', 'PLUMBING PERMIT', 'MECHANICAL PERMIT',
    'HVAC PERMIT', 'GRADING PERMIT', 'DEMOLITION PERMIT', 'SIGN PERMIT',
    'PRESSURE VESSEL PERMIT', 'ELEVATOR PERMIT', 'FIRE SPRINKLER PERMIT',
}

# "License Class: C39    License No.: 807881    Contractor: MODERN ROOFING INC"
RE_LICENCE = re.compile(
    r'License\s+Class:\s*(?P<cls>[A-Z0-9-]*)\s*'
    r'License\s+No\.?:\s*(?P<lic>[A-Z0-9-]*)\s*'
    r'Contractor:\s*(?P<name>[^\r\n]{2,120})', re.I)
RE_OWNER = re.compile(r'^\s*OWNER:\s*(?P<owner>[^\r\n]{2,160})', re.I | re.M)
RE_PERMIT = re.compile(r'Permit\s*#:\s*(?P<nbr>[0-9]{2,6}\s*-\s*[0-9]{3,6}\s*-\s*[0-9]{3,7})', re.I)
RE_SIGNER = re.compile(r'Print\s+Name:\s*(?P<who>[^\r\n]{2,80})', re.I)

lock = threading.Lock()
stats = {'parcels': 0, 'pdfs': 0, 'text': 0, 'contractors': 0, 'failed': 0, 'started': time.time()}


OPENER = None


def build_opener(proxy):
    """LADBS refuses most networks, so requests may need to leave by another route.

    A proxy is taken from --proxy or from HTTPS_PROXY in the environment, and the environment is
    the better place for one carrying a password: it stays out of the command line and out of logs.
    """
    global OPENER
    if proxy:
        handler = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
    else:
        handler = urllib.request.ProxyHandler()      # reads HTTP(S)_PROXY itself
    OPENER = urllib.request.build_opener(handler)
    return OPENER


def get(url, timeout=120, tries=3, binary=False):
    opener = OPENER or urllib.request.build_opener()
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with opener.open(req, timeout=timeout) as r:
                return r.read() if binary else r.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            if attempt == tries - 1:
                return None
            time.sleep(3 * (attempt + 1))
        except Exception:
            if attempt == tries - 1:
                return None
            time.sleep(3 * (attempt + 1))
    return None


def search_parcel(pin, limit=100):
    # Parcel numbers carry runs of spaces ("148-5A207  91"). urlencode would send those as '+',
    # which this service does not read back as a space, and every search then returns nothing.
    q = urllib.parse.urlencode({'pin': pin, 'limit': limit, 'offset': 0},
                               quote_via=urllib.parse.quote)
    body = get(f'{SEARCH}?{q}', timeout=120)
    if not body:
        return []
    try:
        return (json.loads(body).get('hits') or [])
    except Exception:
        return []


def pdf_text(url, tmpdir):
    """Download one permit PDF and return whatever text it carries, or '' when it is a scan."""
    blob = get(url, timeout=180, binary=True)
    if not blob:
        return None
    path = Path(tmpdir) / f'{threading.get_ident()}.pdf'
    path.write_bytes(blob)
    try:
        out = subprocess.run(['pdftotext', '-layout', str(path), '-'],
                             capture_output=True, timeout=120)
        return out.stdout.decode('utf-8', 'replace')
    except Exception:
        return ''
    finally:
        try:
            path.unlink()
        except OSError:
            pass


def read_contractor(text, hit):
    """What the permit says about who did the work."""
    if not text or len(text) < 500:
        return None                      # a scan: nothing to read
    m = RE_LICENCE.search(text)
    owner = RE_OWNER.search(text)
    permit = RE_PERMIT.search(text)
    signer = RE_SIGNER.search(text)
    if not m and not owner:
        return None
    return {
        'permit_number': re.sub(r'\s+', '', permit.group('nbr')) if permit else (hit.get('usr_doc_nbr') or ''),
        'contractor_name': (m.group('name').strip() if m else ''),
        'licence_class': (m.group('cls').strip() if m else ''),
        'licence_number': (m.group('lic').strip() if m else ''),
        'owner': owner.group('owner').strip() if owner else '',
        'signed_by': signer.group('who').strip() if signer else '',
        'doc_type': hit.get('doc_type') or '',
        'sub_type': hit.get('sub_type') or '',
        'doc_date': (hit.get('doc_date') or '')[:10],
        'status': hit.get('status') or '',
        'address': hit.get('address') or '',
        'record_id': hit.get('record_id'),
        'pdf_url': hit.get('pdf_url') or '',
    }


def worker(q, out, pause, tmpdir, max_pdfs):
    while True:
        pin = q.get()
        if pin is None:
            q.task_done()
            return
        time.sleep(pause)
        hits = search_parcel(pin)
        rows, pdfs, text_seen, found = [], 0, 0, 0
        # what the search returned, so a run that finds nothing says which step failed:
        # no documents at all, documents without a pdf, or documents whose type we skip
        seen_types, with_pdf = {}, 0
        for h in hits:
            t = (h.get('doc_type') or '?').upper()
            seen_types[t] = seen_types.get(t, 0) + 1
            if h.get('pdf_url'):
                with_pdf += 1
        for h in hits:
            if pdfs >= max_pdfs:
                break
            url, dt = h.get('pdf_url'), (h.get('doc_type') or '').upper()
            if not url or dt not in WANTED_DOC_TYPES:
                continue
            pdfs += 1
            text = pdf_text(url, tmpdir)
            if text is None:
                continue
            if len(text) >= 500:
                text_seen += 1
            row = read_contractor(text, h)
            if row and row['contractor_name']:
                row['pin'] = pin
                rows.append(row)
                found += 1
        with lock:
            stats['parcels'] += 1
            if not hits:
                stats['empty'] = stats.get('empty', 0) + 1
            stats['pdfs'] += pdfs
            stats['text'] += text_seen
            stats['contractors'] += found
            if not hits:
                stats['failed'] += 1
            for r in rows:
                out.write(json.dumps(r, separators=(',', ':')) + '\n')
            out.write(json.dumps({'pin': pin, '_done': True, 'hits': len(hits),
                                  'with_pdf': with_pdf, 'types': seen_types, 'pdfs': pdfs,
                                  'with_text': text_seen, 'contractors': found},
                                 separators=(',', ':')) + '\n')
            if stats['parcels'] % 50 == 0:
                out.flush()
                rate = stats['parcels'] / max(1, time.time() - stats['started'])
                print(f"  {stats['parcels']:,} parcels ({stats.get('empty', 0):,} empty), {stats['pdfs']:,} pdfs, "
                      f"{stats['text']:,} with text, {stats['contractors']:,} contractors, "
                      f"{rate:.2f} parcels/s", flush=True)
        q.task_done()


def load_pins(path, shard, shards, done):
    """Parcels newest permit first, so the years that still carry text are done before the scans."""
    rows = []
    with path.open(newline='', encoding='utf-8-sig', errors='replace') as f:
        for r in csv.DictReader(f):
            pin, yr = (r.get('pin') or '').strip(), (r.get('newest_year') or '').strip()
            if pin and pin not in done:
                rows.append((yr, pin))
    rows.sort(reverse=True)
    pins = [p for _, p in rows]
    return pins[shard::shards] if shards > 1 else pins


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--pins', type=Path, default=HERE / 'pins.csv',
                    help='csv with columns pin,newest_year')
    ap.add_argument('--out', type=Path, default=HERE / 'prism_contractors.jsonl')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--pause', type=float, default=0.3)
    ap.add_argument('--max-pdfs', type=int, default=12, help='pdfs per parcel')
    ap.add_argument('--shard', default='0/1', help='i/n, so several runs can split the work')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--proxy', default='', help='http://host:port — or set HTTPS_PROXY instead, '
                                                'which keeps any password out of the command line')
    args = ap.parse_args()
    build_opener(args.proxy)

    i, n = (int(x) for x in args.shard.split('/'))
    done = set()
    if args.out.exists():
        with args.out.open(encoding='utf-8') as f:
            for line in f:
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get('_done'):
                    done.add(d['pin'])
    print(f'{len(done):,} parcels already done', flush=True)

    pins = load_pins(args.pins, i, n, done)
    if args.limit:
        pins = pins[:args.limit]
    print(f'shard {i}/{n}: {len(pins):,} parcels to do, {args.workers} workers', flush=True)

    tmpdir = os.environ.get('RUNNER_TEMP') or os.environ.get('TMP') or '.'
    q = queue.Queue(maxsize=args.workers * 4)
    with args.out.open('a', encoding='utf-8') as out:
        threads = [threading.Thread(target=worker,
                                    args=(q, out, args.pause, tmpdir, args.max_pdfs), daemon=True)
                   for _ in range(args.workers)]
        for t in threads:
            t.start()
        for p in pins:
            q.put(p)
        for _ in threads:
            q.put(None)
        q.join()
        out.flush()

    print(f"\ndone: {stats['parcels']:,} parcels, {stats['pdfs']:,} pdfs, "
          f"{stats['text']:,} carried text, {stats['contractors']:,} contractors named", flush=True)


if __name__ == '__main__':
    sys.exit(main())
