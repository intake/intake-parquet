import fastparquet
import os
import pytest

from intake_parquet import ParquetSource

here = os.path.dirname(__file__)
path = os.path.join(here, 'test.parq')
path2 = os.path.join(here, 'test2.parq')
pf = fastparquet.ParquetFile(path)
data = next(pf.iter_row_groups())
data2 = pf.to_pandas()


@pytest.mark.parametrize('url', [path,
                                 [os.path.join(path, 'part.0.parquet'),
                                  os.path.join(path, 'part.1.parquet')],
                                 path2])
@pytest.mark.parametrize('columns', [None,
                                     ['bhello', 'f', 'i32'],
                                     ['bhello']])
def test_source(url, columns):
    if columns:
        d = data[columns]
        d2 = data2[columns]
    else:
        d = data
        d2 = data2
    source = ParquetSource(url, parquet_kwargs=dict(columns=columns))
    source.discover()
    assert source.npartitions == 2
    assert source.shape == d2.shape
    assert source.dtype['bhello'] == 'object'
    part0 = source.read_partition(0)
    assert len(part0) == 1001
    assert part0.equals(d)
    it = source.read_chunked()
    assert next(it).equals(part0)
    assert next(it).equals(part0)
    with pytest.raises(StopIteration):
        next(it)
    with pytest.raises(IndexError):
        source.read_partition(5)
    parts = source.read()
    assert parts.equals(d2)
