# Intake-parquet

[![Build Status](https://travis-ci.org/ContinuumIO/intake-parquet.svg?branch=master)](https://travis-ci.org/ContinuumIO/intake-parquet)
[![Documentation Status](https://readthedocs.org/projects/intake-parquet/badge/?version=latest)](http://intake-parquet.readthedocs.io/en/latest/?badge=latest)

[Intake data loader](https://github.com/ContinuumIO/intake/) interface to the parquet binary tabular data format.

Parquet is very popular in the big-data ecosystem, because it provides columnar
and chunk-wise access to the data, with efficient encodings and compression. This makes
the format particularly effective for streaming through large subsections of even
larger data-sets, hence it's common use with Hadoop and Spark.

Parquet data may be single files, directories of files, or nested directories, where
the directory names are meaningful in the partitioning of the data.

### Features

The parquet plugin allows for:

- efficient metadata parsing, so you know the data types and number of records without
  loading any data
- random access of partitions
- column and index selection, load only the data you need
- passing of value-based filters, that you only load those partitions containing some
  valid data (NB: does not filter the values within a partition)

### Installation

The conda install instructions are:

```
conda install -c intake intake-parquet
conda install fastparquet
```

### Examples

See the notebook in the examples/ directory.
