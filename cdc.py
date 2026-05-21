"""CDC for FTS parser. LUAS: (fts_year, name, vat) — one commitment per beneficiary per year."""

import io, json, uuid, hashlib
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_lib

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.string()),
    ("detected_time", pa.string()),
    ("details_json", pa.string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])

TRACKED_FIELDS = ["contracted_amount", "programme", "department", "subject", "funding_type"]

SNAPSHOT_SCHEMA = pa.schema([
    ("fts_year", pa.int32()),
    ("orgnr", pa.string()),
    ("name", pa.string()),
    ("content_hash", pa.string()),
] + [(f, pa.string()) for f in TRACKED_FIELDS])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("n_commitments", pa.int32()),
    ("programmes", pa.string()),
])


class FTSCDC:

    def __init__(self, bucket_name, prefix="fts"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def _gcs_path(self, *parts):
        return "/".join([self._prefix] + list(parts))

    def _read_parquet(self, path):
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return pq.read_table(io.BytesIO(blob.download_as_bytes()))

    def _write_parquet(self, table, path):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        buf.seek(0)
        self._bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")

    def _list_parsed_dates(self):
        prefix = self._gcs_path("parsed") + "/"
        dates = set()
        for blob in self._bucket.list_blobs(prefix=prefix):
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.add(name.replace(".parquet", ""))
        return sorted(dates)

    def _load_previous_parsed(self, run_date):
        dates = [d for d in self._list_parsed_dates() if d < run_date]
        if not dates:
            return {}
        t = self._read_parquet(self._gcs_path("parsed", f"{dates[-1]}.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        result = {}
        for i in range(t.num_rows):
            key = (d["fts_year"][i], d["orgnr"][i], d["name"][i])
            result[key] = {"content_hash": d["content_hash"][i]}
            for f in TRACKED_FIELDS:
                if f in d:
                    result[key][f] = d[f][i]
        return result

    def _load_pool(self):
        t = self._read_parquet(self._gcs_path("cdc", "pool.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        return {d["orgnr"][i]: {"first_seen": d["first_seen"][i], "last_seen": d["last_seen"][i],
                                "n_commitments": d["n_commitments"][i], "programmes": d["programmes"][i]} for i in range(t.num_rows)}

    def run(self, resolved_rows, unresolved_rows, run_date, run_mode="daily"):
        run_id = str(uuid.uuid4())[:8]
        detected_time = datetime.now(timezone.utc).isoformat()
        old_snaps = self._load_previous_parsed(run_date)
        pool = self._load_pool()
        changelog_rows = []
        new_count = 0
        mod_count = 0
        new_snaps = {}

        for row in resolved_rows:
            key = (row["fts_year"], row["orgnr"], row["name"])
            h = row["content_hash"]
            snap = {"fts_year": row["fts_year"], "orgnr": row["orgnr"], "name": row["name"], "content_hash": h}
            for f in TRACKED_FIELDS:
                snap[f] = str(row.get(f) or "")
            new_snaps[key] = snap

            old_entry = old_snaps.get(key)
            if run_mode == "bootstrap" or old_entry is None:
                event_type = "new"
                changed_fields = None
                new_count += 1
            elif old_entry["content_hash"] != h:
                event_type = "modified"
                diffs = [f for f in TRACKED_FIELDS if str(row.get(f) or "") != str(old_entry.get(f) or "")]
                changed_fields = json.dumps(diffs) if diffs else json.dumps(["content_hash"])
                mod_count += 1
            else:
                continue

            amt = row.get("contracted_amount")
            amt_str = f"EUR {amt:,.0f}" if amt and isinstance(amt, (int, float)) else ""
            summary = " — ".join(filter(None, [row.get("programme", ""), row.get("name", ""), amt_str]))

            details = {k: v for k, v in row.items() if k not in ("content_hash", "orgnr_resolution_method")}
            doc_hash = hashlib.sha256(f"{row['orgnr']}|{row['name']}".encode()).hexdigest()[:12]
            changelog_rows.append({
                "orgnr": row["orgnr"],
                "document_id": f"fts-{row['fts_year']}-{doc_hash}",
                "data_source": "fts",
                "event_type": event_type,
                "event_subtype": f"fts_{row.get('funding_type', 'grant').lower().replace(' ', '_')}",
                "summary": summary,
                "changed_fields": changed_fields,
                "valid_time": row.get("start_date", f"{row['fts_year']}-01-01") if row.get("start_date") and row["start_date"] != "" else f"{row['fts_year']}-01-01",
                "detected_time": detected_time,
                "details_json": json.dumps(details, ensure_ascii=False, default=str),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

            orgnr = row["orgnr"]
            prog = row.get("programme", "")
            if orgnr in pool:
                pool[orgnr]["last_seen"] = run_date
                pool[orgnr]["n_commitments"] += 1
                existing = set(pool[orgnr]["programmes"].split(",")) if pool[orgnr]["programmes"] else set()
                existing.add(prog)
                pool[orgnr]["programmes"] = ",".join(sorted(existing))
            else:
                pool[orgnr] = {"first_seen": run_date, "last_seen": run_date, "n_commitments": 1, "programmes": prog}

        if run_mode != "bootstrap":
            for key, old_h in old_snaps.items():
                if key not in new_snaps:
                    doc_hash = hashlib.sha256(f"{key[1]}|{key[2]}".encode()).hexdigest()[:12]
                    changelog_rows.append({
                        "orgnr": key[1], "document_id": f"fts-{key[0]}-{doc_hash}",
                        "data_source": "fts", "event_type": "disappeared",
                        "event_subtype": "fts_commitment_ended", "summary": f"Disappeared: {key[2][:40]} FY{key[0]}",
                        "changed_fields": None, "valid_time": run_date, "detected_time": detected_time,
                        "details_json": None, "source_run_mode": run_mode, "run_id": run_id,
                    })

        if changelog_rows:
            self._write_parquet(pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA),
                               self._gcs_path("cdc", "changelog", f"{run_date}.parquet"))

        snap_rows = list(new_snaps.values())
        if snap_rows:
            self._write_parquet(pa.Table.from_pylist(snap_rows, schema=SNAPSHOT_SCHEMA),
                               self._gcs_path("parsed", f"{run_date}.parquet"))
        if pool:
            self._write_parquet(pa.Table.from_pylist([{"orgnr": k, **v} for k, v in pool.items()], schema=POOL_SCHEMA),
                               self._gcs_path("cdc", "pool.parquet"))
        if unresolved_rows:
            ur_schema = pa.schema([("fts_year", pa.int32()), ("name", pa.string()), ("vat", pa.string()),
                                   ("programme", pa.string()), ("contracted_amount", pa.string())])
            ur = [{"fts_year": r["fts_year"], "name": r["name"], "vat": r["vat"],
                   "programme": r.get("programme", ""), "contracted_amount": str(r.get("contracted_amount", ""))} for r in unresolved_rows]
            self._write_parquet(pa.Table.from_pylist(ur, schema=ur_schema),
                               self._gcs_path("unresolved", f"{run_date}.parquet"))

        return {"new": new_count, "modified": mod_count, "changelog_rows": len(changelog_rows),
                "pool_size": len(pool), "snapshot_size": len(new_snaps), "unresolved": len(unresolved_rows)}
