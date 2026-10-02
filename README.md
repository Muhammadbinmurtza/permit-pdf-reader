# permit-pdf-reader

Reads the contractor out of a municipal building permit PDF.

Los Angeles publishes 1.6 million permits and names a contractor on none of them, but it publishes
each permit as a PDF, and permits issued through the city's e-permit system carry the contractor in
a text layer: name, licence class, licence number. This walks a list of parcels, fetches each
parcel's permit documents, and reads those fields out with `pdftotext`. No OCR, so scanned older
permits yield nothing and are skipped.

    python collector/prism_contractors.py --pins pins.csv --shard 0/20 --workers 10

`--shard i/n` splits the work so several runs can proceed side by side. Every parcel already in the
output is skipped on restart.
