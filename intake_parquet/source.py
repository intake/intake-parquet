import fsspec
from intake.source import base

from . import __version__


class ParquetSource(base.DataSource):
    """
    Source to load parquet datasets.

    Produces a dataframe.

    A parquet dataset may be a single file, a set of files in a single
    directory or a nested set of directories containing data-files.

    The implementation uses either fastparquet or pyarrow, select with the
    `engine=` kwarg.

    Common keyword parameters accepted by this Source:

    - columns: list of str or None
        column names to load. If None, loads all

    - filters: list of tuples
        row-group level filtering; a tuple like ``('x', '>', 1)`` would mean
        that if a row-group has a maximum value less than 1 for the column
        ``x``, then it will be skipped. Row-level filtering is *not*
        performed.

    - engine: 'fastparquet' or 'pyarrow'
        Which backend to read with.

    - see pd.read_parquet and dd.read_parquet() for the other named parameters that
      can be passed through.
    """
    container = 'dataframe'
    name = 'parquet'
    version = __version__
    partition_access = True

    def __init__(self, urlpath, metadata=None,
                 storage_options=None, **parquet_kwargs):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._kwargs = parquet_kwargs or {}
        self._df = None

        super(ParquetSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            engine = self._kwargs.get("engine", "fastparquet")
            fs, _, _ = fsspec.core.get_fs_token_paths(self._urlpath, **self._storage_options)
            if engine == "fastparquet":
                import fastparquet
                pf = fastparquet.ParquetFile(self._urlpath, fs=fs)
                lc = len(self._kwargs.get("columns") or []) or len(pf.columns)
                dt = {k: str(v) for k, v in pf.dtypes.items()}
                schema = base.Schema(dtype=dt, shape=(pf.count(), lc),
                                npartitions=len(pf.row_groups), extra_metadata=pf.key_value_metadata)
            else:
                import pyarrow.parquet as pq
                pf = pq.ParquetDataset(self._urlpath, filesystem=fs)
                lc = len(self._kwargs.get("columns") or []) or len(pf.schema)
                dt = {k: str(v) for k, v in zip(pf.schema.names, pf.schema.types)}
                schema = base.Schema(dtype=dt, shape=(None, lc),
                                npartitions=len(pf.fragments))
        else:
            npartitions = getattr(self._df, "npartitions", None)
            schema = base.Schema(dtype=self._df.dtypes, shape=self._df.shape,
                            npartitions=npartitions)
        return schema

    def _get_partition(self, i):
        return self.to_dask().get_partition(i).compute()

    def read(self):
        """
        Create single pandas dataframe from the whole data-set
        """
        import pandas as pd
        df = pd.read_parquet(self._urlpath, storage_options=self._storage_options,
                             **self._kwargs)
        self._df = df.iloc[:0]
        return df

    def to_spark(self):
        """Produce Spark DataFrame equivalent

        This will ignore all arguments except the urlpath, which will be
        directly interpreted by Spark. If you need to configure the storage,
        that must be done on the spark side.

        This method requires intake-spark. See its documentation for how to
        set up a spark Session.
        """
        from intake_spark.base import SparkHolder
        args = [
            ['read'],
            ['parquet', [self._urlpath]]
        ]
        sh = SparkHolder(True, args, {})
        return sh.setup()

    def to_dask(self):
        import dask.dataframe as dd
        df = dd.read_parquet(self._urlpath, storage_options=self._storage_options,
                             **self._kwargs)
        self._df = df
        return self._df

    def _close(self):
        self._df = None
