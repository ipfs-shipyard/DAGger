#!/bin/bash

set -o pipefail
set -e

xz -vl "$1" | grep Uncompressed | grep -Eo '\(.+ B\)' | tr -cd '[0-9]' | perl -e 'print pack "q>", <>'
xz -dck "$1"
