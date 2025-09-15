import logging
from collections.abc import Generator
from contextlib import asynccontextmanager
from typing import Annotated

import cql2
import duckdb
import orjson
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from enums import MEDIA_TYPE_MAP, OutputFormat
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


def feature_generator(
    con: duckdb.DuckDBPyConnection | duckdb.DuckDBPyRelation,
    geom_column: str,
) -> Generator[bytes]:
    for batch in con.arrow(batch_size=100).to_batches():  # type: ignore
        for record in batch.to_pylist():
            if (geometry := record.pop(geom_column, None)) is not None:
                geometry = orjson.loads(geometry)
            else:
                continue

            yield orjson.dumps(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": record,
                },
                option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
            )


def stream_feature_collection(
    feature_generator: Generator[bytes],
    number_matched: int,
    limit: int,
    offset: int,
) -> Generator[bytes]:
    yield b'{"type":"FeatureCollection","features":['

    for i, feat in enumerate(feature_generator):
        if i > 0:
            yield b"," + feat
        else:
            yield feat

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


def stream_geojsonseq(feature_generator: Generator[bytes]) -> Generator[bytes]:
    for feat in feature_generator:
        yield feat + b"\n"


async def stream_features(
    db: duckdb.DuckDBPyConnection,
    url: str,
    limit: int,
    offset: int,
    geom_column: str,
    bbox: BBox | None = None,
    filter: str | None = None,
    output_format: OutputFormat | None = None,
):
    """
    Stream a paginated GeoJSON FeatureCollection directly from DuckDB.
    """

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

    rel = db.sql(
        f"""SELECT *
FROM read_parquet('{url}')
{filter_stmt if filters else ""}""",
        params=cql_params if filter else None,
    )

    total = (rel.aggregate("COUNT(*) AS total").fetchone() or [0])[0]

    offset = min(offset, max(total - limit, 0))

    filtered = rel.project(
        f"ST_AsGeoJSON({geom_column}) {geom_column}, * EXCLUDE ({geom_column})"
    ).limit(limit, offset=offset)

    # TODO: support GeoJSONSeq, ndjson, csv, etc.

    generator = feature_generator(filtered, geom_column)

    if output_format == OutputFormat.GEOJSON or output_format is None:
        for chunk in stream_feature_collection(generator, total, limit, offset):
            yield chunk
    elif output_format == OutputFormat.GEOJSONSEQ:
        for chunk in stream_geojsonseq(generator):
            yield chunk


@app.get("/features")
async def get_features(
    db: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
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
            db=db,
            url=url,
            limit=limit,
            offset=offset,
            geom_column=geom_column,
            bbox=bbox,
            filter=filter,
            output_format=f,
        ),
        media_type=MEDIA_TYPE_MAP[f],
    )


@app.get("/metadata")
def get_metadata(
    url: str,
    db: duckdb.DuckDBPyConnection = Depends(duckdb_cursor),
):
    print(db.execute(f"DESCRIBE SELECT * FROM read_parquet('{url}');").fetchdf())
