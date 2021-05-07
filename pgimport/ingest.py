
##########################################################################
## Imports
##########################################################################

import uuid
import btrdb
import warnings
from tqdm import tqdm
from btrdb.utils.timez import to_nanoseconds

from pgimport.parse import StreamData

##########################################################################
## Module Variables and Constants
##########################################################################

INSERT_CHUNK_SIZE = 50000
MERGE_POLICIES = ["never", "retain", "replace", "equal"]

##########################################################################
## Warnings
##########################################################################

class NullValuesWarning(UserWarning):
    """
    Alerts users that null values were detected in data to be inserted
    and data will be dropped, as BTrDB cannot accept null values
    """
    pass

class ProgressBarWarning(UserWarning):
    """
    Alerts users that a progress bar will not be displayed because total
    points was not provided
    """
    pass

##########################################################################
## DataIngestor
##########################################################################

class DataIngestor(object):
    """
    Parameters
    ----------
    conn: btrdb.Connection
    merge_policy: str
        merge policy to use when inserting BTrDB points
    total_points: int
        specifies total number of points to be inserted. Used to create a progess bar.
    """
    def __init__(self, conn, merge_policy="never", total_points=None):
        self.conn = conn

        if total_points is None:
            warnings.warn("total points not provided. Progress bar will not be displayed", ProgressBarWarning)
            self.pbar = None
        else:
            self.pbar = tqdm(total=total_points)
        
        if merge_policy in MERGE_POLICIES:
            self.merge_policy = merge_policy
        else:
            raise Exception(f"'{merge_policy}' is not a valid merge policy. Options are: {', '.join(MERGE_POLICIES)}")
    
    @staticmethod
    def _chunk_points(stream, times, values, chunk_size):
        """
        Parameters
        ----------
        stream: btrdb Stream
        times: pd.Series of timestamps, which can be datetime, datetime64, float, str (RFC 2822)
        values: pd.Series of float values
        chunk_size: int
            specifies number of (time, value) pairs to insert at a time
        """
        # drop any null values
        null_positions = values[values.isnull()].index.tolist()
        if len(null_positions) > 0:
            warnings.warn(f"""null values detected in stream {str(stream.uuid)}.
                {len(null_positions)} points were dropped""", NullValuesWarning)
        values.drop(null_positions, inplace=True)
        times.drop(null_positions, inplace=True)

        points = [(to_nanoseconds(t), v) for t, v in zip(times, values)]
        for i in range(0, len(points), chunk_size):
            yield points[i:i + chunk_size]
    
    # NOTE: I moved this into a separate func to make it easier to test
    def _ingest(self, stream, points):
        """
        Parameters
        ----------
        stream: btrdb Stream
        points: list of (time, value) tuples
        """
        stream.insert(points, self.merge_policy)
    
    # NOTE: Ideally this function would listen to a queue and would pick up StreamData
    # objects from the DataParser and insert as they are produced
    def ingest(self, streamdata, chunk_size=None):
        """
        Parameters
        ----------
        streamdata: StreamData
        chunk_size: int
            specifies number of (time, value) pairs to insert at a time
        """
        if not isinstance(streamdata, StreamData):
            raise TypeError(f"StreamData object expected. Received {type(streamdata)}")

        # check if stream exists already, create it if it doesn't
        meta = streamdata.metadata
        streams = self.conn.streams_in_collection(meta.collection, is_collection_prefix=False, tags=meta.tags)

        if len(streams) == 0:
            stream = self.conn.create(uuid.uuid4(), meta.collection, meta.tags, meta.annotations)
        else:
            stream = streams[0]
        
        # convert time and value arrays into list of tuples and split into chunks for insertion
        chunk_size = chunk_size or INSERT_CHUNK_SIZE
        for points in self._chunk_points(stream, streamdata.times, streamdata.values, chunk_size):
            self._ingest(stream, points)
            if self.pbar:
                self.pbar.update(len(points))