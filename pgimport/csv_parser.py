##########################################################################
## Imports
##########################################################################

import os
import re
import csv
import json
import yaml
import s3fs     # allows pandas.read_csv() to work with s3 uris
import boto3
import pandas as pd
from pathlib import Path

from pgimport.parse import DataParser, Stream, File, Metadata

##########################################################################
## Module Variables and Constants
##########################################################################

# NOTE: metadata can also be provided via a json or yaml file
METADATA = {
    "Freq": {"tags": {"name": "Freq", "unit": "Hz"},
            "annotations": {}},
    "PhA_Vmag": {"tags": {"name": "PhA_Vmag", "unit": "volts"},
                "annotations": {"phase": "A"}},
    "PhA_Vang": {"tags": {"name": "PhA_Vang", "unit": "degrees"},
                "annotations": {"phase": "A"}},
    "PhB_Vmag": {"tags": {"name": "PhB_Vmag", "unit": "volts"},
                "annotations": {"phase": "B"}},
    "PhB_Vang": {"tags": {"name": "PhB_Vang", "unit": "degrees"},
                "annotations": {"phase": "B"}},
    "PhC_Vmag": {"tags": {"name": "PhC_Vmag", "unit": "volts"},
                "annotations": {"phase": "C"}},
    "PhC_Vang": {"tags": {"name": "PhC_Vang", "unit": "degrees"},
                "annotations": {"phase": "C"}},
    "PhA_Cmag": {"tags": {"name": "PhA_Cmag", "unit": "amps"},
            "annotations": {"phase": "A"}},
    "PhA_Cang": {"tags": {"name": "PhA_Cang", "unit": "degrees"},
                "annotations": {"phase": "A"}},
    "PhB_Cmag": {"tags": {"name": "PhB_Cmag", "unit": "amps"},
                "annotations": {"phase": "B"}},
    "PhB_Cang": {"tags": {"name": "PhB_Cang", "unit": "degrees"},
                "annotations": {"phase": "B"}},
    "PhC_Cmag": {"tags": {"name": "PhC_Cmag", "unit": "amps"},
                "annotations": {"phase": "C"}},
    "PhC_Cang": {"tags": {"name": "PhC_Cang", "unit": "degrees"},
                "annotations": {"phase": "C"}},
    "P": {"tags": {"name": "P", "unit": "MW"},
            "annotations": {}},
    "Q": {"tags": {"name": "Q", "unit": "MVAR"},
            "annotations": {}}
}

##########################################################################
## Helper Functions
##########################################################################

def parse_metadata(meta_file):
    """
    Parameters
    ----------
    meta_file: str
        specifies path to metadata file
    
    Returns
    -------
    dict:
        metadata dict result of yaml/json loading
    """
    if meta_file.endswith(".json"):
        try:
            return json.load(open(meta_file))
        except json.JSONDecodeError as e:
            raise Exception(f"error parsing json into Metadata: {e}")
    elif meta_file.endswith(".yaml"):
        try:
            return yaml.load(open(meta_file), Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            raise Exception(f"error parsing yaml into Metadata: {e}")
    raise Exception("Could not parse provided metadata. Valid metadata formats are (dict, json, yaml)")

# NOTE: This is an example of a UDF that is meant to take in any parameters it needs to
# to parse a metadata dict/file and return a Metadata object for each Stream. Any function
# that returns a Metadata object will suffice
def get_metadata(metadata, stream_name, collection):
    """
    Parameters
    ----------
    metadata: dict or str
        either a dict of metadata or a str filename referring to a yaml/json metadata file
    stream_name: str
        name of stream. Used as a key to look up metadata
    collection: str
        specifies the collection that the stream belongs to
    
    Returns
    -------
    pgimport.parse.Metadata object
    """
    # parse metadata into dict if it isn't already
    meta = metadata if isinstance(metadata, dict) else parse_metadata(metadata)
    
    tags = meta[stream_name].get("tags", None)
    annotations = meta[stream_name].get("annotations", None)
    return Metadata(collection, tags, annotations)

##########################################################################
## CSV Parser
##########################################################################

class CSVParser(DataParser):
    def __init__(self, fpath=None, collection_prefix=None, regex=None, metadata=None, meta_func=None):
        """
        Parameters
        ----------
        fpath: str 
            path to directory containing data
        collection_prefix: str
            prefix to add to all streams' collection names
        regex: str
            regex string to use to get collection from file name
        metadata: dict/str:
            either a dict of metadata or a str filename referring to a yaml/json metadata file
        meta_func: func
            callback function to use to map metadata to Stream objects
        """
        # TODO: allow user to specify local path or s3 bucket 
        # somehow detect if meant to be using s3 and connect if so
        self.path = fpath
        self.collection_prefix = collection_prefix
        self.regex = regex if regex else "PMU[\d]*"
        self.meta = metadata if metadata else METADATA
        self.meta_func = meta_func if meta_func else get_metadata
    
    def collect_files(self):
        """
        Recursively searches provided directory and returns a CSVFile object for each csv

        Returns
        -------
        fobjs: list
            list of CSVFile objects referring to csv files that contain data to ingest
        """
        return [File(str(fpath)) for fpath in Path(self.path).rglob(f"*.csv")]
    
    def _parse_collection(self, fname):
        """
        Parameters
        ----------
        fname: str
            name of file
        
        Returns
        -------
        str: collection name
        """
        match = re.search(self.regex, fname)
        if match:
            return "/".join([self.collection_prefix, match.group(0)]) if self.collection_prefix else match.group(0)
        raise Exception(f"could not determine collection to use for file {fname}")
    
    def create_streams(self, files):
        """
        Parameters
        ----------
        files: list
            list of CSVFile objects
        
        Yields
        ------
        streams: list
            list of Stream objects, yielded as chunks per file
        """
        for file in files:
            if not isinstance(file, File):
                raise TypeError(f"Expected File inputs. Received {type(file)}")

            streams = []
            df = pd.read_csv(file.path)
            count = len(df)
            # TODO: relies on timestamps being in the first column
            # probs should add some checking to verify
            times = df.iloc[:,0]
            collection = self._parse_collection(file.path)

            yield [
                Stream(times, df[col], self.meta_func(self.meta, col, collection), count)
                for col in df.columns[1:]
            ]