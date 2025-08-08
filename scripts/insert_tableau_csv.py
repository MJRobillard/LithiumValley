#!/usr/bin/env python3
"""
Insert a CSV into a Tableau .twb workbook as a new textscan datasource and
attach it to all worksheets. Designed to be minimally invasive and reversible
(backs up the workbook before writing changes).

Usage:
  python scripts/insert_tableau_csv.py \
    "Tableau Notebooks/Default Workbook.twb" \
    "data/BLM_CA_GEOTHERMAL_LEASES/BLM_CA_Geothermal_Leases_Polygon.csv" \
    --caption "BLM CA Geothermal Leases (Polygon)" \
    --windows-root C:\\Users\\ratth\\Downloads\\LithiumValley

Notes:
- Tableau .twb stores OS-specific paths in the textscan connection; provide
  a Windows-style base path via --windows-root when running from WSL/Linux.
- The script infers column types from a small sample; adjust in Tableau if needed.
python scripts/insert_tableau_csv.py "Tableau Notebooks/Default Workbook.twb"
   "data/BLM_CA_GEOTHERMAL_LEASES/BLM_CA_Geothermal_Leases_Polygon.csv" --caption "BLM CA Geothermal        
   Leases (Polygon)" --windows-root "C:\Users\ratth\Downloads\LithiumValley"
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import uuid
import xml.etree.ElementTree as ET
from typing import List, Tuple


def to_windows_path(path: str, windows_root: str | None) -> str:
    """Convert a path to Windows style suitable for Tableau textscan directory.

    If running in WSL and path starts with /mnt/<drive>/, map to <DRIVE>:\\...
    Otherwise, if --windows-root is provided, make the CSV path relative to it
    and join with the provided root to form an absolute Windows path.
    """
    norm = os.path.normpath(path)
    # Ensure absolute path first
    if not os.path.isabs(norm):
        norm = os.path.abspath(norm)
    # WSL mount form: /mnt/c/Users/... -> C:\Users\...
    if norm.startswith("/mnt/"):
        parts = norm.split(os.sep)
        if len(parts) > 3:
            drive = parts[2].upper()
            rest = os.path.join(*parts[3:])
            return f"{drive}:{os.sep}{rest}"
    if windows_root:
        # Build a Windows absolute path using provided root if CSV is under CWD
        try:
            rel = os.path.relpath(norm, start=os.getcwd())
            if not rel.startswith(".."):
                return os.path.join(windows_root, rel).replace("/", os.sep)
        except Exception:
            pass
    # Default: absolute with Windows separators
    return norm.replace("/", os.sep)


def split_dir_filename_windows(win_path: str) -> Tuple[str, str]:
    directory, filename = os.path.split(win_path)
    # Normalize to backslashes
    directory = directory.replace("/", os.sep)
    return directory, filename


def guess_type(samples: List[str]) -> str:
    """Guess Tableau type for a column from sample strings: integer, real, string."""
    def is_int(s: str) -> bool:
        return bool(re.fullmatch(r"[+-]?\d+", s))

    def is_real(s: str) -> bool:
        return bool(re.fullmatch(r"[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?", s))

    only_non_empty = [s for s in samples if s not in ("", None)]
    if not only_non_empty:
        return "string"
    if all(is_int(s) for s in only_non_empty):
        return "integer"
    if all(is_real(s) for s in only_non_empty):
        return "real"
    return "string"


def sample_csv_columns(csv_path: str, max_rows: int = 500) -> Tuple[List[str], List[str]]:
    """Return (headers, tableau_types) by sampling up to max_rows rows."""
    headers: List[str] = []
    samples: List[List[str]] = []

    # Try UTF-8 first, then fallback to latin-1
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(csv_path, "r", newline="", encoding=encoding, errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader)
                # Ensure headers are unique and non-empty
                headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]
                for _ in range(max_rows):
                    try:
                        row = next(reader)
                    except StopIteration:
                        break
                    # Pad/truncate to headers length
                    row = (row + [""] * len(headers))[: len(headers)]
                    samples.append(row)
                break
        except Exception:
            continue
    if not headers:
        raise RuntimeError(f"Unable to read CSV headers from {csv_path}")

    # Transpose samples per column
    col_samples: List[List[str]] = [[] for _ in headers]
    for row in samples:
        for i, val in enumerate(row):
            col_samples[i].append(val.strip())

    types = [guess_type(col) for col in col_samples]
    return headers, types


def make_table_attr(filename: str) -> str:
    """Build the Tableau table attribute like [Base#csv]."""
    base, ext = os.path.splitext(filename)
    ext_no_dot = ext[1:].lower() if ext.startswith(".") else ext.lower()
    return f"[{base}#{ext_no_dot}]"


def ensure_datasources_node(root: ET.Element) -> ET.Element:
    ds = root.find("datasources")
    if ds is None:
        ds = ET.Element("datasources")
        # Insert after <preferences> if present, else at top under <workbook>
        prefs = root.find("preferences")
        if prefs is not None:
            # Insert right after preferences
            parent = root
            children = list(parent)
            idx = children.index(prefs)
            parent.insert(idx + 1, ds)
        else:
            root.insert(0, ds)
    return ds


def add_textscan_datasource(root: ET.Element, csv_path: str, caption: str | None, windows_root: str | None) -> Tuple[str, str]:
    """Add a new textscan datasource for the CSV. Returns (datasource_name, caption)."""
    ds_container = ensure_datasources_node(root)

    win_path = to_windows_path(csv_path, windows_root)
    directory, filename = split_dir_filename_windows(win_path)
    ds_name = f"textscan.{uuid.uuid4().hex[:30]}"
    ds_caption = caption or filename

    # If a textscan datasource with the same filename already exists, update it
    for existing in ds_container.findall("datasource"):
        conn = existing.find("connection")
        if conn is None:
            continue
        if conn.get("class") != "textscan":
            continue
        existing_filename = conn.get("filename")
        if existing_filename and existing_filename == filename:
            # Update directory and relation
            conn.set("directory", directory)
            conn.set("filename", filename)
            rel = conn.find("relation")
            if rel is not None:
                rel.set("name", filename)
                rel.set("table", make_table_attr(filename))
            if ds_caption:
                existing.set("caption", ds_caption)
            existing_name = existing.get("name")
            if not existing_name:
                # Ensure a valid name exists
                existing_name = ds_name
                existing.set("name", existing_name)
            return existing_name, existing.get("caption", ds_caption)

    datasource = ET.Element("datasource", attrib={
        "caption": ds_caption,
        "inline": "true",
        "name": ds_name,
        "version": "18.1",
    })

    connection = ET.SubElement(datasource, "connection", attrib={
        "class": "textscan",
        "directory": directory,
        "filename": filename,
        "password": "",
        "server": "",
    })

    relation = ET.SubElement(connection, "relation", attrib={
        "name": filename,
        "table": make_table_attr(filename),
        "type": "table",
    })

    # Add column metadata for better initial experience
    headers, types = sample_csv_columns(csv_path)
    columns = ET.SubElement(relation, "columns", attrib={
        "character-set": "UTF-8",
        "header": "yes",
        "locale": "en_US",
        "separator": ",",
    })
    for i, (h, t) in enumerate(zip(headers, types)):
        ET.SubElement(columns, "column", attrib={
            "datatype": t,
            "name": h,
            "ordinal": str(i),
        })

    # Optionally include an empty <aliases enabled='yes' /> to match Tableau style
    ET.SubElement(datasource, "aliases", attrib={"enabled": "yes"})

    # Append to <datasources>
    ds_container.append(datasource)

    return ds_name, ds_caption


def attach_datasource_to_all_worksheets(root: ET.Element, ds_name: str, ds_caption: str) -> int:
    """Ensure each worksheet's view has a reference to the datasource."""
    count = 0
    for ws in root.findall("worksheets/worksheet"):
        view = ws.find("table/view")
        if view is None:
            continue
        datasources = view.find("datasources")
        if datasources is None:
            datasources = ET.SubElement(view, "datasources")
        # Check if already attached
        already = any(d.get("name") == ds_name for d in datasources.findall("datasource"))
        if not already:
            ET.SubElement(datasources, "datasource", attrib={
                "caption": ds_caption,
                "name": ds_name,
            })
            count += 1
    return count


def backup_file(path: str) -> str:
    base, ext = os.path.splitext(path)
    backup = f"{base}.backup{ext}"
    if not os.path.exists(backup):
        with open(path, "rb") as src, open(backup, "wb") as dst:
            dst.write(src.read())
    return backup


def main() -> None:
    parser = argparse.ArgumentParser(description="Insert a CSV into a Tableau .twb as a new textscan datasource.")
    parser.add_argument("workbook", help="Path to .twb workbook")
    parser.add_argument("csv", help="Path to .csv file to insert")
    parser.add_argument("--caption", help="Datasource caption (defaults to CSV filename)")
    parser.add_argument("--windows-root", help="Windows base path to build the textscan directory (use when running from WSL)")
    args = parser.parse_args()

    wb_path = args.workbook
    csv_path = args.csv

    if not os.path.exists(wb_path):
        print(f"Error: workbook not found: {wb_path}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(csv_path):
        print(f"Error: csv not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    # Parse XML
    tree = ET.parse(wb_path)
    root = tree.getroot()

    # Backup before changes
    backup_file(wb_path)

    # Inject datasource and attach to worksheets
    ds_name, ds_caption = add_textscan_datasource(root, csv_path, args.caption, args.windows_root)
    attached = attach_datasource_to_all_worksheets(root, ds_name, ds_caption)

    # Write back (preserve UTF-8 without XML declaration change)
    tree.write(wb_path, encoding="utf-8", xml_declaration=True)

    print(f"Inserted datasource: {ds_caption} ({ds_name})")
    print(f"Attached to worksheets: {attached}")
    print("Done. Open the workbook in Tableau to validate fields and set roles.")


if __name__ == "__main__":
    main()
