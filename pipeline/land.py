"""STAGE 1 - LAND: fetch raw, preserve unchanged. No cleaning here."""
import json, os
from datetime import datetime, timezone
import pandas as pd


def _read_xlsx(manifest):
    """Adapter: read each configured sheet into raw records."""
    src = manifest["source"]
    out = {}
    for key, cfg in src["sheets"].items():
        df = pd.read_excel(src["path"], sheet_name=cfg["name"],
                           header=cfg["header_row"])
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
        df = df.dropna(axis=0, how="all")
        # everything -> string: preserves zip leading zeros, dates, precision
        out[key] = json.loads(df.astype(object).where(df.notna(), None)
                                .astype(str).replace("None", None).to_json(orient="records"))
    return out


def _read_csv(manifest):
    """Adapter: read a CSV file into raw records."""
    src = manifest["source"]
    df = pd.read_csv(src["path"])
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df = df.dropna(axis=0, how="all")
    return {"patients": json.loads(
        df.astype(object).where(df.notna(), None)
        .to_json(orient="records"))}

ADAPTERS = {"xlsx": _read_xlsx, "csv": _read_csv}


def land(manifest, out_root="data"):
    adapter = ADAPTERS[manifest["source"]["type"]]
    datasets = adapter(manifest)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = os.path.join(out_root, "raw", manifest["tenant_id"], stamp)
    os.makedirs(base, exist_ok=True)

    written = {}
    for name, rows in datasets.items():
        dest = os.path.join(base, f"{name}.json")
        with open(dest, "w") as f:
            json.dump(rows, f, indent=2)
        written[name] = (len(rows), dest)
    return datasets, written