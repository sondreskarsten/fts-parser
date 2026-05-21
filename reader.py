"""GCS reader for FTS parser. Reads raw XLSX from snapshots."""

import io, json
from google.cloud import storage as gcs_lib


class GCSReader:

    def __init__(self, bucket_name, prefix="fts"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def list_snapshot_dates(self):
        prefix = f"{self._prefix}/raw/"
        dates = set()
        iterator = self._bucket.list_blobs(prefix=prefix, delimiter="/")
        for page in iterator.pages:
            for p in page.prefixes:
                d = p.rstrip("/").split("/")[-1]
                if len(d) == 10:
                    dates.add(d)
        return sorted(dates)

    def load_manifest(self, snapshot_date):
        blob = self._bucket.blob(f"{self._prefix}/raw/{snapshot_date}/manifest.json")
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def read_xlsx_bytes(self, snapshot_date, year):
        path = f"{self._prefix}/raw/{snapshot_date}/{year}_FTS_dataset_en.xlsx"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return blob.download_as_bytes()
