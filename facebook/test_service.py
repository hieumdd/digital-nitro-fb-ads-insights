import pytest

from facebook.pipeline import pipelines
from facebook.facebook_service import pipeline_service, tasks_service
from facebook.accounts import accounts

TIMEFRAME = [
    ("auto", (None, None)),
    # ("manual", ("2022-01-01", "2022-11-01")),
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


@pytest.mark.parametrize(
    "account",
    accounts,
    ids=lambda account: account.ads_account_id,
)
def test_pipeline_service(pipeline, account, timeframe):
    res = pipeline_service(pipeline, account.ads_account_id, *timeframe)
    assert res


def test_tasks_service(timeframe):
    res = tasks_service(*timeframe)
    assert res
