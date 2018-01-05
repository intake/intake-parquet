from functools import partial
from glob import glob

from dask.delayed import delayed
import dask.dataframe as dd

from intake.source import base

import fastparquet as fp

__version__ = '0.0.1'


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(
            name='fastparquet', version=__version__, container='dataframe',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return FastparquetSource(urlpath=urlpath, pcap_kwargs=source_kwargs,
                                 metadata=base_kwargs['metadata'])


class FastparquetSource(base.DataSource):
    def __init__(self, urlpath, kwargs, metadata):
        self._init_args = dict(kwargs=kwargs, metadata=metadata)

        self._kwargs = kwargs
        self._dataframe = None

        super(self.__class__, self).__init__(
            container='dataframe', metadata=metadata)

    def _get_dataframe(self):
        return self._dataframe

    def discover(self):
        self._get_dataframe()

    def read(self):
        return self._get_dataframe().compute()

    def read_chunked(self):
        df = self._get_dataframe()

        for i in range(df.npartitions):
            yield df.get_partition(i).compute()

    def read_partition(self, i):
        df = self._get_dataframe()
        return df.get_partition(i).compute()

    def to_dask(self):
        return self._get_dataframe()

    def close(self):
        self._dataframe = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
