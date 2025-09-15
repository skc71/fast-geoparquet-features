import csv
import logging
from collections.abc import Generator
from contextlib import asynccontextmanager
from typing import Annotated, Any

import cql2
import duckdb
import orjson
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from enums import MediaType, OutputFormat
from models import BBox

logger = logging.getLogger("uvicorn")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set application lifespan variables including:
    * A reusable DuckDB connection
    """
    con = duckdb.connect()
    extensions = ["httpfs", "azure", "aws", "spatial"]
    con.execute("\n".join(f"INSTALL {ext}; LOAD {ext};" for ext in extensions))

    app.state.db = con
    yield
    app.state.db.close()


app = FastAPI(
    title="FastFeatures",
    lifespan=lifespan,
)


def feature_generator(
    con: duckdb.DuckDBPyConnection | duckdb.DuckDBPyRelation,
    geom_column: str,
) -> Generator[dict[str, Any]]:
    """Yield GeoJSON like Features from an Arrow Table.

    Attempts to parse geometry column as JSON. If an error
    occurs, it is left as string (e.g., WKT for CSV output).
    """
    for batch in con.arrow(batch_size=100).to_batches():  # type: ignore
        for record in batch.to_pylist():
            if (geometry := record.pop(geom_column, None)) is not None:
                try:
                    geometry = orjson.loads(geometry)
                except orjson.JSONDecodeError:
                    pass
            else:
                continue

            yield {
                "type": "Feature",
                "geometry": geometry,
                "properties": record,
            }


def dump_feat(feat: dict[str, Any]) -> bytes:
    return orjson.dumps(
        feat,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
    )


def stream_feature_collection(
    features: Generator[dict[str, Any]],
    number_matched: int,
    limit: int,
    offset: int,
) -> Generator[bytes]:
    yield b'{"type":"FeatureCollection","features":['

    for i, feat in enumerate(features):
        if i > 0:
            yield b"," + dump_feat(feat)
        else:
            yield dump_feat(feat)

    metadata = (
        orjson.dumps(
            {
                "numberMatched": number_matched,
                "numberReturned": i + 1,
                "limit": limit,
                "offset": offset,
            }
        )
        .decode()
        .strip("{}")
    )
    yield f"], {metadata}}}".encode()


def stream_geojsonseq(features: Generator[dict[str, Any]]) -> Generator[bytes]:
    for feat in features:
        yield dump_feat(feat) + b"\n"


def stream_csv(features: Generator[dict[str, Any]]) -> Generator[bytes]:
    """Cribbed from TiPG:
    https://github.com/developmentseed/tipg/blob/b9aff728e857b9d40b56f315d91aa8b6ab397f8f/tipg/factory.py#L100
    """

    class DummyWriter:
        """Dummy writer that implements write for use with csv.writer."""

        def write(self, line: str):
            """Return line."""
            return line

    row = next(features)
    columns = row.keys()

    writer = csv.DictWriter(DummyWriter(), fieldnames=columns)

    yield writer.writerow(dict(zip(columns, columns)))

    yield writer.writerow(row)

    for row in features:
        yield writer.writerow(row)


def base_rel(
    *,
    con: duckdb.DuckDBPyConnection,
    url: str,
    bbox: BBox | None,
    filter: str | None,
) -> duckdb.DuckDBPyRelation:
    filters = list()

    if bbox is not None:
        filters.append(bbox.to_sql())

    cql_filter = None
    cql_params = None
    if filter:
        cql_filter = cql2.parse_text(filter).to_sql()
        filters.append(cql_filter.query)
        cql_params = cql_filter.params

    filter_stmt = f"WHERE {' AND '.join(filters)}"

    rel = con.sql(
        f"""SELECT *
FROM read_parquet('{url}')
{filter_stmt if filters else ""}""",
        params=cql_params if filter else None,
    )
    return rel


def get_count(rel: duckdb.DuckDBPyRelation) -> int:
    return (rel.aggregate("COUNT(*) AS total").fetchone() or [0])[0]


async def stream_features(
    con: duckdb.DuckDBPyConnection,
    url: str,
    limit: int,
    offset: int,
    geom_column: str,
    bbox: BBox | None = None,
    filter: str | None = None,
    output_format: OutputFormat | None = None,
):
    """Stream features from GeoParquet."""
    rel = base_rel(
        con=con,
        url=url,
        bbox=bbox,
        filter=filter,
    )
    total = get_count(rel)

    offset = min(offset, max(total - limit, 0))

    geom_conversion_func = (
        "ST_AsText" if output_format in [OutputFormat.CSV] else "ST_AsGeoJSON"
    )

    filtered = rel.project(
        (
            f"{geom_conversion_func}({geom_column}) {geom_column}, "
            f"* EXCLUDE ({geom_column})"
        )
    ).limit(limit, offset=offset)

    # TODO: support GeoJSONSeq, ndjson, csv, etc.

    generator = feature_generator(filtered, geom_column)
    if output_format == OutputFormat.GEOJSON or output_format is None:
        stream = stream_feature_collection(generator, total, limit, offset)
    elif output_format in [OutputFormat.GEOJSONSEQ, OutputFormat.NDJSON]:
        stream = stream_geojsonseq(generator)
    elif output_format == OutputFormat.CSV:
        stream = stream_csv(generator)

    for chunk in stream:
        yield chunk


def duckdb_cursor(request: Request) -> duckdb.DuckDBPyConnection:
    """Returns a threadsafe cursor from the connection stored in app state."""
    return request.app.state.db.cursor()


def parse_bbox(bbox: str | None = None) -> BBox | None:
    if bbox is None:
        return None

    try:
        return BBox.from_str(bbox)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )


@app.get(
    "/features",
    responses={
        status.HTTP_200_OK: {
            "content": {
                MediaType.GEOJSON: {},
                MediaType.GEOJSONSEQ: {},
            }
        }
    },
)
async def get_features(
    con: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
    url: str = Query(),
    limit: int = Query(
        default=10,
        gte=1,
        lte=10_000,
    ),
    offset: int = Query(default=0, ge=0),
    geom_column: str = Query(default="geometry", alias="geom-column"),
    filter: str | None = Query(None),
    bbox: Annotated[BBox, str] | None = Depends(parse_bbox),
    f: OutputFormat = OutputFormat.GEOJSON,
):
    """Get Features"""

    return StreamingResponse(
        stream_features(
            con=con,
            url=url,
            limit=limit,
            offset=offset,
            geom_column=geom_column,
            bbox=bbox,
            filter=filter,
            output_format=f,
        ),
        media_type=MediaType[f.name],
    )


@app.get("/metadata")
def get_metadata(
    url: str,
    db: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
):
    print(db.execute(f"DESCRIBE SELECT * FROM read_parquet('{url}');").fetchdf())
