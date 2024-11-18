# CIFF to DuckDB

## Introduction

The Common Index File Format (CIFF) was introduced as a binary data exchange format for open-source search engines 
to interoperate by sharing index structures.

CIFF has been adopted by the [OpenWebSearch.EU](https://openwebsearch.eu) project to distribute (partitions of) 
Web indexes.

This repository provides the code necessary to load a CIFF file through Arrow into DuckDB.

The goal is to load and transform the CIFF data into an index for the DuckDB Full Text Search extension.
_(The version provided has not yet completely achieved that goal.)_

## Preliminaries

Install DuckDB CLI for testing:

    wget https://artifacts.duckdb.org/latest/duckdb-binaries-linux.zip
    unzip -p duckdb-binaries-linux.zip duckdb_cli-linux-amd64.zip | funzip > ./duckdb ; chmod a+rx ./duckdb

## Upload Index

    python ciff-arrow.py

## Reclaim space

The process executes a few SQL commands after loading the base tables from the Protobuf representation.

The resulting overhead in disk storage can be reduced with a simple command:

    duckdb < reclaim-space.sql

See the [footprint docs](https://duckdb.org/docs/operations_manual/footprint_of_duckdb/reclaiming_space.html).
