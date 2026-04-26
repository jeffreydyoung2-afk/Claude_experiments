"""
Search a set of directories for .xls/.xlsx/.xlsb files whose name contains a given
stub, look inside them for CASE_IDs from an input list, and write a CSV of
(CASE_ID, SOURCE_FILE) pairs for every match found.

Edit the CONFIG section below, then run:  python3 find_case_ids.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

# ---------- CONFIG ---------------------------------------------------------

DIRECTORIES = [
    "/path/to/dir1",
    "/path/to/dir2",
]

FILE_STUB = "report"                         # substring that must appear in the filename
CASE_ID_FILE = "case_ids.txt"                # one CASE_ID per line
OUTPUT_FILE = "case_id_matches.csv"
RECURSIVE = True                             # walk sub-directories too
CASE_INSENSITIVE = False                     # match CASE_IDs case-insensitively

# ---------------------------------------------------------------------------


def load_case_ids(path: Path) -> set[str]:
    ids = {line.strip() for line in path.read_text().splitlines() if line.strip()}
    if CASE_INSENSITIVE:
        ids = {x.lower() for x in ids}
    return ids


def find_excel_files(directories: list[str], stub: str) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for d in directories:
        root = Path(d)
        if not root.is_dir():
            print(f"warning: not a directory, skipping: {root}", file=sys.stderr)
            continue
        iterator = root.rglob("*") if RECURSIVE else root.glob("*")
        for p in iterator:
            if not p.is_file():
                continue
            if p.suffix.lower() not in (".xls", ".xlsx", ".xlsb"):
                continue
            if stub not in p.name:
                continue
            resolved = p.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(p)
    return files


def case_ids_in_file(path: Path, case_ids: set[str]) -> set[str]:
    """Return the subset of case_ids that appear anywhere in any sheet of the file."""
    engine = "pyxlsb" if path.suffix.lower() == ".xlsb" else None
    try:
        sheets = pd.read_excel(
            path, sheet_name=None, dtype=str, header=None, engine=engine
        )
    except Exception as e:
        print(f"warning: failed to read {path}: {e}", file=sys.stderr)
        return set()

    found: set[str] = set()
    for df in sheets.values():
        if df.empty:
            continue
        values = df.stack(dropna=True).astype(str).map(str.strip)
        if CASE_INSENSITIVE:
            values = values.str.lower()
        present = set(values.unique()) & case_ids
        found.update(present)
        if found == case_ids:
            break
    return found


def main() -> int:
    case_id_path = Path(CASE_ID_FILE)
    if not case_id_path.is_file():
        print(f"error: CASE_ID file not found: {case_id_path}", file=sys.stderr)
        return 1

    case_ids = load_case_ids(case_id_path)
    if not case_ids:
        print("error: no CASE_IDs loaded from input file", file=sys.stderr)
        return 1

    files = find_excel_files(DIRECTORIES, FILE_STUB)
    print(f"scanning {len(files)} file(s) for {len(case_ids)} CASE_ID(s)...")

    matches: list[tuple[str, str]] = []
    for f in files:
        hits = case_ids_in_file(f, case_ids)
        for cid in sorted(hits):
            matches.append((cid, str(f)))

    matches.sort()

    out_path = Path(OUTPUT_FILE)
    with out_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["CASE_ID", "SOURCE_FILE"])
        writer.writerows(matches)

    found_ids = {cid for cid, _ in matches}
    missing = case_ids - found_ids
    print(f"wrote {len(matches)} match row(s) to {out_path}")
    print(f"matched {len(found_ids)}/{len(case_ids)} CASE_IDs; {len(missing)} not found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
