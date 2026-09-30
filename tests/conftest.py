import pytest

from src.session import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark("Day16-tests", shuffle_partitions=2)
    yield session
    session.stop()
