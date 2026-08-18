#!/bin/bash
#
# Builds the KV server and the load generator.
#
# C++20 is required by modern libpqxx (7.9+), whose headers use concepts,
# std::ranges and std::source_location.  Our own code is C++17-clean; the
# standard level is raised for the dependency, not for us.
#
set -euo pipefail

CXX=${CXX:-g++}
STD=${STD:-c++20}
CXXFLAGS="-std=${STD} -O2 -Wall -Wextra -pthread"

echo "==> resolving dependencies (${STD})"

# --- libpqxx / libpq -------------------------------------------------------
if pkg-config --exists libpqxx 2>/dev/null; then
    PQXX_CFLAGS=$(pkg-config --cflags libpqxx)
    PQXX_LIBS=$(pkg-config --libs libpqxx)
elif command -v brew >/dev/null 2>&1; then
    BREW=$(brew --prefix)
    PQXX_CFLAGS="-I${BREW}/opt/libpqxx/include -I${BREW}/opt/libpq/include"
    PQXX_LIBS="-L${BREW}/opt/libpqxx/lib -L${BREW}/opt/libpq/lib -lpqxx -lpq"
else
    PQXX_CFLAGS=""
    PQXX_LIBS="-lpqxx -lpq"
fi

# --- libcurl (load generator only) -----------------------------------------
if pkg-config --exists libcurl 2>/dev/null; then
    CURL_CFLAGS=$(pkg-config --cflags libcurl)
    CURL_LIBS=$(pkg-config --libs libcurl)
elif command -v brew >/dev/null 2>&1 && [ -d "$(brew --prefix)/opt/curl" ]; then
    BREW=$(brew --prefix)
    CURL_CFLAGS="-I${BREW}/opt/curl/include"
    CURL_LIBS="-L${BREW}/opt/curl/lib -lcurl"
else
    CURL_CFLAGS=""
    CURL_LIBS="-lcurl"
fi

echo "==> building server"
# shellcheck disable=SC2086
$CXX $CXXFLAGS $PQXX_CFLAGS server.cpp -o server $PQXX_LIBS

echo "==> building loadgen"
# shellcheck disable=SC2086
$CXX $CXXFLAGS $CURL_CFLAGS loadgen.cpp -o loadgen $CURL_LIBS

echo "==> done: ./server ./loadgen"