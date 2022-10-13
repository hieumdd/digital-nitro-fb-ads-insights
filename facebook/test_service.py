import pytest

from facebook.pipeline import pipelines
from facebook.facebook_service import pipeline_service, tasks_service

TIMEFRAME = [
    # ("auto", (None, None)),
    ("manual", ("2022-01-01", "2022-10-01")),
]


@pytest.fixture(params=[i[1] for i in TIMEFRAME], ids=[i[0] for i in TIMEFRAME])
def timeframe(request):
    return request.param


@pytest.fixture(  # type: ignore
    params=pipelines.values(),
    ids=lambda x: x.name,
)
def pipeline(request):
    return request.param


def test_pipeline_service(pipeline, timeframe):
    res = pipeline_service(pipeline, *timeframe)
    assert res

def test_tasks_service(timeframe):
    res = tasks_service(*timeframe)
    assert res
