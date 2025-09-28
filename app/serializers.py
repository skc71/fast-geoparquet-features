import csv
from collections.abc import Generator
from typing import Any
from urllib.parse import urlencode

import orjson
from fastapi import Request

from app.enums import MediaType
from app.models import Link


def dump_feat(feat: dict[str, Any]) -> bytes:
    return orjson.dumps(
        feat,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
    )


def build_links(
    request: Request,
    number_matched: int,
    limit: int,
    offset: int,
) -> list[Link]:
    params: dict[str, Any] = request.query_params._dict.copy()
    links = [
        Link(
            title="Features",
            rel="self",
            href=request.url._url,
            type=MediaType.GEOJSON,
        )
    ]

    base_url = request.url_for("get_features")._url

    if (next_offset := (offset + limit)) < number_matched:
        params["offset"] = next_offset
        links.append(
            Link(
                title="Next page",
                rel="next",
                href=f"{base_url}?{urlencode(params)}",
                type=MediaType.GEOJSON,
            )
        )

    if offset > 0:
        params["offset"] = max(offset - limit, 0)
        links.append(
            Link(
                title="Previous page",
                rel="prev",
                href=f"{base_url}?{urlencode(params)}",
                type=MediaType.GEOJSON,
            )
        )

    return links


def stream_feature_collection(
    features: Generator[dict[str, Any]],
    number_matched: int,
    limit: int,
    offset: int,
    request: Request,
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
                "links": [
                    link.model_dump()
                    for link in build_links(request, number_matched, limit, offset)
                ],
            }
        )
        .decode()
        .strip("{")
    )
    yield f"], {metadata}".encode()


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
