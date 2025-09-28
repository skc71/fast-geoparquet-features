import orjson
import pytest

from app.serializers import stream_csv, stream_feature_collection, stream_geojsonseq


@pytest.fixture
def feature_generator():
    def _feature_generator():
        for coords in [[0, 0], [0, 0], [0, 0]]:
            yield {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": coords,
                },
                "properties": {},
            }

    return _feature_generator()


def test_csv(feature_generator):
    output = [row for row in stream_csv(feature_generator)]

    assert output[0] == "type,geometry,properties\r\n"
    assert all(
        row == "Feature,\"{'type': 'Point', 'coordinates': [0, 0]}\",{}\r\n"
        for row in output[1:]
    )


def test_stream_featurecollection(feature_generator):
    output = orjson.loads(
        b"".join(stream_feature_collection(feature_generator, 3, 3, 10, 0, []))
    )

    assert output["type"] == "FeatureCollection"
    assert "numberMatched" in output
    assert "numberReturned" in output
    assert "limit" in output
    assert "offset" in output
    assert "links" in output


def test_stream_geojsonseq(feature_generator):
    output = [feature for feature in stream_geojsonseq(feature_generator)]
    assert all(orjson.loads(feat)["type"] == "Feature" for feat in output)
