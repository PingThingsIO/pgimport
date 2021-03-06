# pgimport

A python based tool for importing data into PingThings' PredictiveGrid platform.

## Install/Setup

`pgimport` is not currently hosted on a package management server, so you will need to install it directly from GitHub (or install from a local copy). To have pip download the codebase and install, you can run the following provided that you have GitHub configured with an SSH key pair:

```
pip install git+ssh://git@github.com/PingThingsIO/pgimport
```

else via:

```
pip install git+https://github.com/PingThingsIO/pgimport
```

## Concept Overview

`pgimport` splits the task of data ingestion into two processes. The first process is handled by `DataParsers`, which are responsible for locating files containing raw data and parsing that data into `StreamData` objects. `StreamData` contains arrays of timestamps and values, as well as metadata (collection name, tags, annotations). `StreamData` objects are passed to `DataIngestors`, which are responsible for mapping `StreamData` objects to BTrDB streams (or creating a new stream if it doesn't exist yet), and inserting points.

Due to the unique nature of most data ingestions with regard to data location, layout and file format, it is expected that users will need to create custom data parsers by implementing the `DataParser` interface. The `DataParser` interface needs to implement two methods: `collect_files()` and `instantiate_streams()`. These methods work together to locate and process raw data files and turn them into `StreamData` objects that can be handled by the `DataIngestor`.

![](images/flow.png)

## Basic Usage

Below is an example of how to use an example parser that I created to ingest csv files:

```python
import os
import btrdb

from pgimport.csv_parser import MyCSVParser
from pgimport.ingest import DataIngestor

# instantiate CSVParser with local path for stream data and collection prefix
cp = MyCSVParser(fpath="test_csvs/", collection_prefix="test_ingest")

# locate files and calculate total number of points
files = cp.collect_files()

# Connect to BTrDB, instantiate ingestor and insert data
# NOTE: this requires providing valid btrdb credentials
conn = btrdb.connect()

ingestor = DataIngestor(conn)
for streams in cp.instantiate_streams(files):
    ingestor.ingest(streams)
```
