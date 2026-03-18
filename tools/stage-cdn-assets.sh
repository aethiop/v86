#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

shopt -s nullglob

paths=()

paths+=(bios/*.bin)
paths+=(images/*.bin images/*.bin.zst images/*.img images/*.iso images/*.json images/*.txt)

if [[ -d images/arch ]]; then
    paths+=(images/arch)
fi

if [[ -d images/alpine-rootfs-flat ]]; then
    paths+=(images/alpine-rootfs-flat)
fi

if ((${#paths[@]} == 0)); then
    echo "No CDN assets found to stage."
    exit 0
fi

git add -f "$@" "${paths[@]}"
git status --short -- images bios
