from intake.source import base

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class Plugin(base.Plugin):
    """
    Intake plugin for reading the parquet format into data-frames
    """
    def __init__(self):
        super(Plugin, self).__init__(
            name='parquet', version=__version__, container='dataframe',
            partition_access=True)

    def open(self, urlpath, **kwargs):
        """
        Create a ParquetSource from the given URL and arguments
        """
        from intake_parquet.source import ParquetSource
        storage_options = kwargs.pop('storage_options', None)
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ParquetSource(urlpath=urlpath, parquet_kwargs=source_kwargs,
                             metadata=base_kwargs['metadata'],
                             storage_options=storage_options)
