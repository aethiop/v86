#!/usr/bin/env python3

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import tempfile
import threading
import urllib.parse
import urllib.request
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download or rewrite filesystem blob assets so they can be committed and served from GitHub/jsDelivr."
    )
    parser.add_argument("--url-list", type=Path, help="Text file containing one blob URL per line")
    parser.add_argument("--output-dir", type=Path, help="Directory where processed blobs will be written")
    parser.add_argument("--base-fs", type=Path, help="Existing fs.json file to rewrite for compressed blobs")
    parser.add_argument("--fs-out", type=Path, help="Output path for the rewritten fs.json")
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress each blob with zstd and append .zst to the stored filename",
    )
    parser.add_argument(
        "--split-size-mb",
        type=int,
        default=95,
        help="Split processed blobs larger than this size into parts (default: 95)",
    )
    parser.add_argument(
        "--split-manifest",
        type=Path,
        help="Where to write split-manifest.json (defaults to <output-dir>/split-manifest.json)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=4,
        help="Number of download workers to run in parallel (default: 4)",
    )
    return parser.parse_args()


def iter_regular_files(nodes):
    for node in nodes:
        if len(node) < 7:
            continue
        target = node[6]
        mode = node[3] & 0o170000

        if isinstance(target, list):
            yield from iter_regular_files(target)
        elif isinstance(target, str) and mode == 0o100000:
            yield node


def rewrite_fs_json(base_fs_path: Path, fs_out_path: Path, suffix: str):
    with base_fs_path.open() as f:
        doc = json.load(f)

    for node in iter_regular_files(doc["fsroot"]):
        if not node[6].endswith(suffix):
            node[6] = node[6] + suffix

    fs_out_path.parent.mkdir(parents=True, exist_ok=True)
    with fs_out_path.open("w") as f:
        json.dump(doc, f, separators=(",", ":"))


def download_file(url: str, destination: Path):
    req = urllib.request.Request(url, headers={"User-Agent": "v86-cdn-publisher"})
    with urllib.request.urlopen(req) as response, destination.open("wb") as out:
        shutil.copyfileobj(response, out)


def compress_file(src: Path, dest: Path):
    subprocess.run(
        ["zstd", "-19", "--quiet", "-f", "-o", str(dest), str(src)],
        check=True,
    )


def split_file(path: Path, split_size_bytes: int):
    parts = []
    with path.open("rb") as src:
        index = 0
        while True:
            chunk = src.read(split_size_bytes)
            if not chunk:
                break

            part_name = f"{path.name}.part.{index:04d}"
            part_path = path.with_name(part_name)
            with part_path.open("wb") as part_file:
                part_file.write(chunk)

            parts.append(part_name)
            index += 1

    path.unlink()
    return parts


def process_url(url, output_dir, split_size_bytes, compress, manifest, manifest_lock):
    url = url.strip()
    if not url:
        return "skip-empty"

    basename = Path(urllib.parse.urlparse(url).path).name
    target_name = basename + ".zst" if compress and not basename.endswith(".zst") else basename
    target_path = output_dir / target_name

    with manifest_lock:
        existing_parts = manifest.get(target_name)

    if existing_parts:
        if all((output_dir / part).exists() for part in existing_parts):
            return f"skip split {target_name}"

    if target_path.exists():
        return f"skip file {target_name}"

    with tempfile.TemporaryDirectory(prefix="cdn-blobs-") as tmpdir:
        tmpdir = Path(tmpdir)
        downloaded = tmpdir / basename
        processed = tmpdir / target_name

        download_file(url, downloaded)

        if compress:
            compress_file(downloaded, processed)
        else:
            shutil.move(downloaded, processed)

        processed_size = processed.stat().st_size
        if processed_size > split_size_bytes:
            parts = split_file(processed, split_size_bytes)
            for part_name in parts:
                shutil.move(str(tmpdir / part_name), output_dir / part_name)
            with manifest_lock:
                manifest[target_name] = parts
            return f"split {target_name} -> {len(parts)} parts"

        shutil.move(str(processed), target_path)
        with manifest_lock:
            manifest.pop(target_name, None)
        return f"wrote {target_name}"


def main():
    args = parse_args()

    if args.output_dir is None and args.url_list is not None:
        raise SystemExit("--output-dir is required when --url-list is used")

    if bool(args.base_fs) != bool(args.fs_out):
        raise SystemExit("--base-fs and --fs-out must be used together")

    output_dir = args.output_dir
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = args.split_manifest
    if manifest_path is None and output_dir is not None:
        manifest_path = output_dir / "split-manifest.json"

    manifest = {}
    if manifest_path and manifest_path.exists():
        with manifest_path.open() as f:
            manifest = json.load(f)

    split_size_bytes = args.split_size_mb * 1024 * 1024
    manifest_lock = threading.Lock()

    if args.url_list:
        urls = [line.strip() for line in args.url_list.read_text().splitlines() if line.strip()]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
            futures = [
                pool.submit(
                    process_url,
                    url,
                    output_dir,
                    split_size_bytes,
                    args.compress,
                    manifest,
                    manifest_lock,
                )
                for url in urls
            ]
            for future in concurrent.futures.as_completed(futures):
                print(future.result())

    if manifest_path:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w") as f:
            json.dump(dict(sorted(manifest.items())), f, indent=2, sort_keys=True)
            f.write("\n")
        print(f"wrote manifest {manifest_path}")

    if args.base_fs and args.fs_out:
        suffix = ".zst" if args.compress else ""
        rewrite_fs_json(args.base_fs, args.fs_out, suffix)
        print(f"wrote fs json {args.fs_out}")


if __name__ == "__main__":
    main()
