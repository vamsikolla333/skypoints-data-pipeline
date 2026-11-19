import pytest
from src.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("SkyPointsTests")
    yield session
    session.stop()