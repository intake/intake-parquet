import fastparquet
import glob
import msgpack
import os
import pickle
import pytest
import shutil

from intake_parquet.source import ParquetSource
import intake

# intake.register_driver('parquet', ParquetSource, clobber=True)  # because pytest defers import
here = os.path.dirname(__file__)
path = os.path.join(here, 'test.parq')
path2 = os.path.join(here, 'test2.parq')
pf = fastparquet.ParquetFile(path)
data = next(pf.iter_row_groups())
data2 = pf.to_pandas()


@pytest.mark.parametrize('url', [path,
                                 [os.path.join(path, 'part.0.parquet'),
                                  os.path.join(path, 'part.1.parquet')],
                                 ])
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
    source = ParquetSource(url, columns=columns, engine="fastparquet")
    source.discover()
    assert source.npartitions == 2
    assert source.shape == (2000, len(d2.columns))
    assert source.dtype['bhello'] == 'object'
    part0 = source.read_partition(0)
    assert len(part0) == 1000
    assert part0.reset_index(drop=True).equals(d)
    it = source.read_chunked()
    assert next(it).reset_index(drop=True).equals(part0)
    assert next(it).reset_index(drop=True).equals(part0)
    with pytest.raises(StopIteration):
        next(it)
    with pytest.raises(IndexError):
        source.read_partition(5)
    parts = source.read()
    assert parts.reset_index(drop=True).equals(d2)


def test_discover_after_dask():
    source = ParquetSource(path)
    d = source.discover()
    df = source.to_dask()
    d2 = source.discover()
    assert isinstance(d['dtype'], dict)  # maybe should be zero-length df
    assert set(d['dtype']) == set(df.columns)  # order may not match for dict
    assert d2['dtype'] == d['dtype']
    assert d2['shape'] == d['shape']
    # dask index starts at 0 for each partition
    assert df.compute().reset_index(drop=True).equals(
        source.read().reset_index(drop=True))


@pytest.mark.parametrize("engine", ["fastparquet", "pyarrow"])
def test_discover_serialize(engine):
    source = ParquetSource(path, engine=engine)
    assert msgpack.packb(source.discover())


def test_pickle():
    source = ParquetSource(path)
    d = source.read()
    source2 = pickle.loads(pickle.dumps(source))
    d2 = source2.read()
    assert d.equals(d2)
