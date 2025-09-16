# `fast-geoparquet-features`

A simple GeoParquet feature server built with FastAPI and DuckDB. Query, filter, and serve vector data straight from GeoParquet in object storage.

✨ Features

* 🚀 Serve GeoParquet directly via HTTP endpoints
* ⚡ Fast queries with DuckDB (spatial extension enabled)
* 🗂️ Filter features by bounding box or CQL expressions
* 🌍 GeoJSON, GeoJSONSeq/ndjson, and CSV output formats supported
* 🐍 Modern Python stack with FastAPI + async streaming responses

> [!WARNING]
> This is a tech demo/prototype. Expect bugs and breaking changes.

## Setup

* `uv sync`
* `uv run fastapi dev main.py`
* Open `http://localhost:8000/docs` in your browser to view the interactive Swagger docs

## Examples

Here are some examples of querying Overture Foundation's Buildings dataset directly in S3.

* Bounding box filter:

    ```sh
    $ curl -X 'GET' \
    'http://localhost:8000/features?url=s3%3A%2F%2Foverturemaps-us-west-2%2Frelease%2F2025-08-20.1%2Ftheme%3Dbuildings%2Ftype%3Dbuilding%2F%2A&limit=100&bbox=-73.98407324497613,40.711304868311316,-73.98038796085099,40.713572466980054' | jq > data/demo.geojson
    ```

    * [Result](./data/demo.geojson)
        ![demo](./public/demo.png)

* Bounding box and CQL2-Text filter (`height > 350`):

    ```sh
    $ curl -X 'GET' \
    'http://localhost:8000/features?url=s3%3A%2F%2Foverturemaps-us-west-2%2Frelease%2F2025-08-20.1%2Ftheme%3Dbuildings%2Ftype%3Dbuilding%2F%2A&filter=height%20%3E%20350&f=geojson&bbox=-73.99341797466995%2C40.75292045436345%2C-73.95647120320056%2C40.777695601276434' | jq > data/height-filter-demo.geojson
    ```

    * [Result](./data/height-filter-demo.geojson)

        ![demo](./public/height-filter-demo.png)

## Notes

* Bounding box filtering requires GeoParquet created with bbox/covering metadata as described in [the v1.1.0 spec](https://geoparquet.org/releases/v1.1.0/)
* Performance is best with [a spatially sorted GeoParquet](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet.md)
* The first query of a large and/or partitioned GeoParquet will take significantly longer than subsequent queries due to the initial read of the Parquet metadata (which DuckDB caches for reuse). For example, the first query of the Overture Buildings dataset after a fresh start takes ~30s. After that, the same query is signficantly faster.

## Acknowledgements

This project was inspired by and benefits from some really cool open source projects including:

* [tipg](https://developmentseed.org/tipg/)
* [cql2-rs](https://developmentseed.org/cql2-rs/latest/)
* [duckdb](https://github.com/duckdb/duckdb)
* [duckdb spatial extension](https://github.com/duckdb/duckdb-spatial)
* [GeoParquet](https://github.com/opengeospatial/geoparquet)
