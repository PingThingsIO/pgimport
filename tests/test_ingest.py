import uuid
import pytest
import pandas as pd
from unittest.mock import patch, Mock, MagicMock

import btrdb
from btrdb.utils.timez import to_nanoseconds, ns_delta

from pgimport.ingest import DataIngestor
from pgimport.parse import StreamData, Metadata

NUM_SAMPLES = 4

@pytest.fixture
def conn(scope="session", autouse=True):
    db = MagicMock(btrdb.connect())
    db.streams_in_collection = Mock(return_value=[btrdbstream])
    return db

@pytest.fixture
def btrdbstream(scope="session", autouse=True):
    stream = Mock(btrdb.stream.Stream)
    return stream

@pytest.fixture
def stream(scope="session", autouse=True):
    times = pd.Series([to_nanoseconds("2021-01-01 00:00:00.00") + ns_delta(milliseconds=33*i) for i in range(NUM_SAMPLES)])
    values = pd.Series([float(i) for i in range(NUM_SAMPLES)])
    tags = {"name": "foo", "unit": "bars"}
    meta =  Metadata("fake_collection", tags)
    return StreamData(times, values, meta, NUM_SAMPLES)


@pytest.mark.usefixtures('conn', 'stream')
class TestDataIngestor(object):
    
    @patch("pgimport.ingest.DataIngestor._ingest")
    def test_ingest(self, mock_ingest, conn, stream):
        data = [(t,v) for t,v in zip(stream.times, stream.values)]
        ingestor = DataIngestor(conn, merge_policy="retain", total_points=101)
        ingestor.ingest(stream)
        # just testing that test ingestion ran with expected params for now
        mock_ingest.assert_called_with(btrdbstream, data)
