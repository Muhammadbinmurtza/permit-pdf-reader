"""The contractor on a City of Los Angeles permit, read from the permit's own detail page.

Each permit has a public detail page addressed by its own number, 26016-90000-23958 becoming
id1=26016&id2=90000&id3=23958, and its Contact Information block names the contractor with the
CSLB licence and address on one line:

    Contractor | J E M A Construction Inc; Lic. No.: 835778-B | 5040 HEINTZ STREET  BALDWIN PARK, CA

One request per permit, no search and no PDF. Engineers, architects and owner-builders appear in
the same block and are kept with their role so a reader can tell them apart.

The permit list is newest first and processed in numbered blocks, so successive runs each take the
next block, and each shard writes its own gzipped output so runs never collide.
"""
import argparse, collections, gzip, html, json, re, sys, threading, time, queue
import urllib.error, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
URL = 'https://www.ladbsservices2.lacity.org/OnlineServices/PermitReport/PcisPermitDetail?id1={}&id2={}&id3={}'
HEADERS = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/125.0 Safari/537.36')}
ROLES = {'Contractor', 'Engineer', 'Architect', 'Geologist', 'Landscape Architect', 'Owner',
         'Owner-Builder', 'Applicant', 'Agent', 'Developer', 'Designer'}
LIC = re.compile(r'^(?P<name>.*?);\s*Lic\.\s*No\.?:\s*(?P<lic>[^|]*)$')

lock = threading.Lock()
stats = collections.Counter()
started = time.time()


def page_text(raw):
    t = re.sub(r'<script.*?</script>|<style.*?</style>', '', raw, flags=re.S | re.I)
    t = html.unescape(re.sub(r'<[^>]+>', '|', t))
    t = t.replace('\xa0', ' ')
    t = re.sub(r'[ \t\r\n]*\|[ \t\r\n|]*', '|', t)
    return re.sub(r'[ \t]{2,}', '  ', t)


def contacts(text):
    """The Contact Information block, as (role, name, licence number, licence class, address)."""
    i = text.find('Contact Information|')
    if i < 0:
        return None
    end = len(text)
    for stop in ('|Inspector Information', '|Pending Inspections', '|Inspection Request History'):
        j = text.find(stop, i)
        if 0 <= j < end:
            end = j
    toks = [x.strip() for x in text[i + len('Contact Information|'):end].split('|') if x.strip()]
    out, k = [], 0
    while k < len(toks):
        role = toks[k]
        if role in ROLES and k + 1 < len(toks):
            body = toks[k + 1]
            m = LIC.match(body)
            name, lic = (m.group('name').strip(), m.group('lic').strip()) if m else (body, '')
            num, cls = (lic.split('-', 1) + [''])[:2] if lic else ('', '')
            addr = []
            k += 2
            while k < len(toks) and toks[k] not in ROLES:
                if toks[k] not in (',',):
                    addr.append(toks[k])
                k += 1
            out.append({'role': role, 'name': name, 'licence': num.strip(), 'licence_class': cls.strip(),
                        'address': re.sub(r'\s{2,}', ', ', ' '.join(addr)).strip(' ,')})
        else:
            k += 1
    return out


def fetch(permit, tries=4):
    a, b, c = permit.split('-')
    url = URL.format(a, b, c)
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=60) as r:
                with lock:
                    stats['http_200'] += 1
                return r.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            with lock:
                stats[f'http_{e.code}'] += 1
            if e.code in (403, 429, 503):
                time.sleep(8 * (attempt + 1))         # throttled: wait, do not give up
                continue
            return None
        except Exception:
            with lock:
                stats['neterr'] += 1
            time.sleep(4 * (attempt + 1))
    return None


def worker(q, out, pause):
    while True:
        permit = q.get()
        if permit is None:
            q.task_done()
            return
        time.sleep(pause)
        raw = fetch(permit)
        rec = {'permit': permit}
        if raw is None:
            rec['status'] = 'failed'
        else:
            cs = contacts(page_text(raw))
            if cs is None:
                rec['status'] = 'no_page'          # the not-found shell
            else:
                rec['status'] = 'ok'
                rec['contacts'] = cs
        with lock:
            stats[rec['status']] += 1
            if rec.get('contacts') and any(x['role'] == 'Contractor' and 'owner' not in x['name'].lower()
                                           for x in rec['contacts']):
                stats['named_contractor'] += 1
            out.write(json.dumps(rec, separators=(',', ':')) + '\n')
            n = stats['ok'] + stats['no_page'] + stats['failed']
            if n % 200 == 0:
                print(f"  {n:,} done  ok={stats['ok']:,} contractor={stats['named_contractor']:,} "
                      f"no_page={stats['no_page']:,} failed={stats['failed']:,}  "
                      f"{n / max(1, time.time() - started):.1f}/s  {dict((k, v) for k, v in stats.items() if k.startswith('http'))}",
                      flush=True)
        q.task_done()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--permits', type=Path, default=HERE / 'permits.csv.gz')
    ap.add_argument('--block', type=int, default=0)
    ap.add_argument('--block-size', type=int, default=200000)
    ap.add_argument('--shard', default='0/1')
    ap.add_argument('--workers', type=int, default=3)
    ap.add_argument('--pause', type=float, default=0.3)
    ap.add_argument('--out', type=Path, required=True)
    args = ap.parse_args()

    i, n = (int(x) for x in args.shard.split('/'))
    with gzip.open(args.permits, 'rt', encoding='utf-8') as f:
        f.readline()
        allp = [line.split(',')[0].strip() for line in f]
    block = allp[args.block * args.block_size:(args.block + 1) * args.block_size]
    mine = block[i::n]
    print(f'{len(allp):,} permits in all; block {args.block} holds {len(block):,}; '
          f'shard {i}/{n} takes {len(mine):,}', flush=True)

    q = queue.Queue(maxsize=args.workers * 4)
    with gzip.open(args.out, 'wt', encoding='utf-8') as out:
        ts = [threading.Thread(target=worker, args=(q, out, args.pause), daemon=True)
              for _ in range(args.workers)]
        for t in ts:
            t.start()
        for p in mine:
            q.put(p)
        for _ in ts:
            q.put(None)
        q.join()
    print(f'\ndone: {dict(stats)}  in {time.time() - started:.0f}s', flush=True)


if __name__ == '__main__':
    sys.exit(main())
