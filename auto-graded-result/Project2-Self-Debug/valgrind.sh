#!/bin/bash
valgrind \
--trace-children=yes \
--leak-check=full \
--track-origins=yes \
--soname-synonyms=somalloc="*jemalloc*" \
--error-exitcode=1 \
--suppressions=../../build_support/valgrind.supp \
$1
