import fastparquet
import os
import pytest

from intake_parquet import ParquetSource, Plugin

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


def test_plugin():
    p = Plugin()
    s = p.open(path)
    out = s.read()
    assert out.equals(data2)


def test_discover_after_dask():
    source = ParquetSource(path)
    d = source.discover()
    df = source.to_dask()
    d2 = source.discover()
    assert isinstance(d['dtype'], dict)  # maybe should be zero-length df
    assert set(d['dtype']) == set(df.columns)  # order may not match for dict
    assert d2['dtype'] is df._meta
    assert d2['shape'] == d['shape']
    # dask index starts at 0 for each partition
    assert df.compute().reset_index(drop=True).equals(source.read())


def test_on_s3():
    pytest.importorskip('s3fs')
    s = ParquetSource('s3://MDtemp/gzip-nation.impala.parquet')
    s.read()
    assert s.shape == (25, 4)


def test_filter():
    import tempfile
    import pandas as pd
    import numpy as np
    d = str(tempfile.mkdtemp())
    try:
        df = pd.DataFrame({'a': np.random.randint(10, size=100),
                           'b': np.random.choice(['oi', 'hi'], size=100)})
        df.index.name = 'index'
        fastparquet.write(d, df, partition_on=['b'], file_scheme='hive',
                          write_index=True)
        p = Plugin()
        s = p.open(d, filters=[('b', '==', 'hi')])
        disc = s.discover()
        assert disc['npartitions'] == 1
        # TODO: this fails because the index appears as a column
        # assert disc['shape'] == df[df.b == 'hi'].shape
        out = s.read()
        assert 'oi' not in out.b
        assert df[df.b == 'hi'].a.equals(out.a)
    finally:
        import shutil
        shutil.rmtree(d)
