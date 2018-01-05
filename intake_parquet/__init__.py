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
    def __init__(self, urlpath, parquet_kwargs=None, metadata=None):
        self._urlpath = urlpath
        self._csv_kwargs = parquet_kwargs or {}
        self._pf = None

        super(ParquetSource, self).__init__(container='dataframe',
                                            metadata=metadata)

    def _get_schema(self):
        if self._pf is None:
            self._pf = fp.ParquetFile(self._urlpath)
        pf = self._pf

        return base.Schema(datashape=None,
                           dtype=pf.dtypes,
                           shape=(len(pf.columns), pf.count),
                           npartitions=len(pf.row_groups),
                           extra_metadata=pf.key_value_metadata)

    def _get_partition(self, i):
        pf = self._pf
        index = pf._get_index(None)
        columns = pf.columns
        if index and index not in columns:
            columns.append(index)
        rg = pf.row_groups[i]
        df, views = pf.pre_allocate(rg.num_rows, columns, None, index)
        pf.read_row_group_file(rg, columns, None, index, assign=views)
        return df

    def _close(self):
        self._pf = None
