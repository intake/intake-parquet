import fastparquet
import glob
import msgpack
import os
import pickle
import pytest
import shutil

from intake_parquet.source import ParquetSource
import intake

intake.registry['parquet'] = ParquetSource  # because pytest defers import
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
    source = ParquetSource(url, columns=columns)
    source.discover()
    assert source.npartitions == 2
    assert source.shape == (None, len(d2.columns))
    assert source.dtype['bhello'] == 'object'
    part0 = source.read_partition(0)
    assert len(part0) == 1001
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


def test_discover_serialize():
    source = ParquetSource(path)
    assert msgpack.packb(source.discover())


def test_pickle():
    source = ParquetSource(path)
    d = source.read()
    source2 = pickle.loads(pickle.dumps(source))
    d2 = source2.read()
    assert d.equals(d2)


def test_on_s3():
    pytest.importorskip('s3fs')
    s = ParquetSource('s3://MDtemp/gzip-nation.impala.parquet')
    s.read()
    assert s.shape == (None, 4)


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
        s = ParquetSource(d, filters=[('b', '==', 'hi')])
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


def test_with_cache():
    import tempfile
    d = tempfile.mkdtemp()
    old = intake.config.conf['cache_dir']
    expected = fastparquet.ParquetFile(os.path.join(here, 'split')).to_pandas()
    try:
        intake.config.conf['cache_dir'] = d
        cat = intake.open_catalog(os.path.join(here, 'cache_cat.yaml'))
        s = cat.split()
        assert isinstance(s.cache[0], intake.source.cache.DirCache)
        outfiles = s.cache[0].load(s._urlpath, output=False)
        assert outfiles
        assert outfiles[0].startswith(s.cache_dirs[0])
        loc = s.cache[0]._path(s._urlpath)
        assert glob.glob(loc + '/*/*/*.parquet')
        assert s.read().reset_index(drop=True).equals(expected)
    finally:
        shutil.rmtree(d)
        intake.config.conf['cache_dir'] = old

def test_to_cudf():
    cudf = pytest.importorskip("cudf")
    source = ParquetSource(path2)
    df = source.to_cudf()
    assert df.shape == (2002, 7)
    assert isinstance(df, cudf.DataFrame)
