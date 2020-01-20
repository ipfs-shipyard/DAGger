#!/bin/bash

set -o pipefail
set -e

gzip -l "$1" | tail -n 1 | awk '{ print $2 }' | perl -e 'print pack "q>", <>'
gzip -dck "$1"
