##########################################################################
## Imports
##########################################################################

import abc

##########################################################################
## StreamData, Metadata and File objects
##########################################################################

class StreamData(object):
    """
    Parameters
    ----------
    times: pandas.Series
        contains timestamps, which can be datetime, datetime64, float, str (RFC 2822)
    values: pandas.Series
        contains float values
    metadata: Metadata
        Metadata object that contains a stream's collection, tags, annotations
    count: int
        total number of points in stream
    """
    def __init__(self, times, values, metadata, count):
        self.times = times
        self.values = values
        self.metadata = metadata
        self.count = count

class Metadata(object):
    """
    Parameters
    ----------
    collection: str
    tags: dict
        specifies tags for a stream. Can only contain (name, unit, ingress, distiller)
    annotations: dict
        specifies annotations for a stream. Can contain any key/values or be empty
    """
    def __init__(self, collection, tags, annotations=None):
        self.collection = collection
        if not isinstance(tags, dict) or (annotations and not isinstance(annotations, dict)):
            raise TypeError("tags and annotations must be provided as dicts")
        # it's okay for a stream to have empty annotations
        self.annotations = annotations or {}
        self.tags = tags

class File(object):
    """
    Parameters
    ----------
    path: str
        specifies path to file (either local path or s3 uri)
    """
    def __init__(self, path):
        self.path = path

##########################################################################
## DataParser Interface
##########################################################################

class DataParser(metaclass=abc.ABCMeta):
    
    @abc.abstractmethod
    def collect_files(self):
        """
        This method can take in any parameters it needs to and is meant to return 
        a list of Files

        Returns
        -------
        list of File objects
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def instantiate_streams(self, files):
        """
        This method parses Files and should return or yield lists of Streams

        Parameters
        ----------
        files: list of File objects

        Returns
        -------
        streams: list of Stream objects
        """
        raise NotImplementedError