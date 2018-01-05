from dask.delayed import delayed
import dask.dataframe as dd

from intake.source import base

import fastparquet as fp

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
    def __init__(self, urlpath, parquet_kwargs, metadata):
        self._urlpath = urlpath
        self._csv_kwargs = parquet_kwargs
        self._dataframe = None

        super(ParquetSource, self).__init__(container='dataframe',
                                            metadata=metadata)

    def _get_schema(self):
        if self._pf is None:
            self._pf = fp.ParquetFile(self.urlpath)
        pf = self._pf

        return base.Schema(datashape=None,
                           dtype=pf.dtypes,
                           shape=(len(pf.columns), pf.count),
                           npartitions=len(pf.rgs),
                           extra_metadata=pf.key_value_metadata)

    def _get_partition(self, i):
        pf = self._pf
        index = pf._get_index(None)
        columns = pf.columns
        if index and index not in columns:
            columns.append(index)
        rg = pf.row_groups[i]
        df, views = self.pre_allocate(rg.num_rows, columns,
                                      None, index)
        return pf.read_row_group_file(rg, columns, None, index,
                                      assign=views)

    def _close(self):
        self._dataframe = None
