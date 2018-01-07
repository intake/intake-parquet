import fastparquet as fp
from intake.source import base

__version__ = '0.0.1'


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(
            name='parquet', version=__version__, container='dataframe',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ParquetSource(urlpath=urlpath, parquet_kwargs=source_kwargs,
                             metadata=base_kwargs['metadata'])


class ParquetSource(base.DataSource):
    """
    Source to load parquet datasets.

    Produces a dataframe.

    A parquet dataset may be a single file, a set of files in a single
    directory or a nested set of directories containing data-files.

    Current implementation uses fastparquet: URL should either point to
    a single file, a directory containing a `_metadata` file, or a list of
    data files.

    Keyword parameters accepted by this Source:
    - columns: list of str or None
        column names to load. If None, loads all
    - index: str or None
        column to make into the index of the dataframe. If None, may be
        inferred from the saved matadata in certain cases.
    """

    def __init__(self, urlpath, parquet_kwargs=None, metadata=None):
        self._urlpath = urlpath
        self._kwargs = parquet_kwargs or {}
        self._pf = None

        super(ParquetSource, self).__init__(container='dataframe',
                                            metadata=metadata)

    def _get_schema(self):
        if self._pf is None:
            self._pf = fp.ParquetFile(self._urlpath)
        pf = self._pf
        columns = self._kwargs.get('columns', None)
        if columns:
            dtypes = {k: v for k, v in pf.dtypes.items() if k in columns}
        else:
            dtypes = pf.dtypes

        return base.Schema(datashape=None,
                           dtype=dtypes,  # one of these is the index
                           shape=(pf.count, len(dtypes)),
                           npartitions=len(pf.row_groups),
                           extra_metadata=pf.key_value_metadata)

    def _get_partition(self, i):
        pf = self._pf
        index = pf._get_index(self._kwargs.get('index', None))
        columns = self._kwargs.get('columns', None)
        columns = columns if columns else pf.columns
        if index and index not in columns:
            columns.append(index)
        rg = pf.row_groups[i]
        df, views = pf.pre_allocate(rg.num_rows, columns, None, index)
        pf.read_row_group_file(rg, columns, None, index, assign=views)
        return df

    def read(self):
        # More efficient to use `to_pandas` directly.
        self._load_metadata()
        columns = self._kwargs.get('columns', None)
        index = self._kwargs.get('index', None)
        return self._pf.to_pandas(columns=columns, index=index)

    def to_dask(self):
        # More efficient to call dask function directly.
        import dask.dataframe as dd
        self._load_metadata()
        columns = self._kwargs.get('columns', None)
        index = self._kwargs.get('index', None)
        return dd.read_csv(self._urlpath, columns=columns, index=index)

    def _close(self):
        self._pf = None
