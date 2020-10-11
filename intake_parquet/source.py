from intake.source import base
from . import __version__


class ParquetSource(base.DataSource):
    """
    Source to load parquet datasets.

    Produces a dataframe.

    A parquet dataset may be a single file, a set of files in a single
    directory or a nested set of directories containing data-files.

    The implementation uses either fastparquet, pyarrow or cudf, select with
    the `engine=` kwarg.

    Keyword parameters accepted by this Source:

    - columns: list of str or None
        column names to load. If None, loads all

    - index: str or None
        column to make into the index of the dataframe. If None, may be
        inferred from the saved matadata in certain cases.

    - filters: list of tuples
        row-group level filtering; a tuple like ``('x', '>', 1)`` would mean
        that if a row-group has a maximum value less than 1 for the column
        ``x``, then it will be skipped. Row-level filtering is *not*
        performed.

    - engine: 'fastparquet', 'pyarrow' or 'cudf'
        Which backend to read with.

    - gather_statistics : bool or None (default).
        Gather the statistics for each dataset partition. By default,
        this will only be done if the _metadata file is available. Otherwise,
        statistics will only be gathered if True, because the footer of
        every file will be parsed (which is very slow on some systems).

    - see dd.read_parquet() for the other named parameters that can be passed through.
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
            self._df = self._to_dask()
        dtypes = {k: str(v) for k, v in self._df._meta.dtypes.items()}
        self._schema = base.Schema(datashape=None,
                                   dtype=dtypes,
                                   shape=(None, len(self._df.columns)),
                                   npartitions=self._df.npartitions,
                                   extra_metadata={})
        return self._schema

    def _get_partition(self, i):
        self._get_schema()
        return self._df.get_partition(i).compute()

    def read(self):
        """
        Create single pandas dataframe from the whole data-set
        """
        self._load_metadata()
        return self._df.compute()

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
        self._load_metadata()
        return self._df

    def _to_dask(self):
        """
        Create a lazy dask-dataframe from the parquet data
        """
        import dask.dataframe as dd
        urlpath = self._get_cache(self._urlpath)[0]
        self._df = dd.read_parquet(urlpath,
                                   storage_options=self._storage_options, **self._kwargs)
        self._load_metadata()
        return self._df

    def to_cudf(self):
        """
        Load a Parquet dataset into a GPU-backed cudf.DataFrame
        """
        import cudf

        self._df = cudf.read_parquet(filepath_or_buffer=self._urlpath, **self._kwargs)
        return self._df

    def _close(self):
        self._df = None
