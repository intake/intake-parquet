"""
Minimal spatial test copied from datashader
"""
import os
import pytest
import numpy as np
import pandas as pd

from intake_parquet.source import ParquetSource
import intake

pytest.importorskip('datashader')

intake.registry['parquet'] = ParquetSource  # because pytest defers import

@pytest.fixture()
def df():
    N = 1000
    np.random.seed(25)

    df = pd.DataFrame({
        'x': np.random.rand(N),
        'y': np.random.rand(N) * 2,
        'a': np.random.randn(N)
    })

    # Make sure we have x/y values of 0 and 1 represented so that
    # autocomputed ranges are predictable
    df.x.iloc[0] = 0.0
    df.x.iloc[-1] = 1.0
    df.y.iloc[0] = 0.0
    df.y.iloc[-1] = 2.0
    return df

@pytest.fixture(params=[False, True])
def s_points_frame(request, tmp_path, df):
    import datashader.spatial.points as dsp

    # Work around https://bugs.python.org/issue33617
    tmp_path = str(tmp_path)
    p = 5
    path = os.path.join(tmp_path, 'spatial_points.parquet')

    dsp.to_parquet(
        df, path, 'x', 'y', p=p, npartitions=10)

    spf = ParquetSource(path).to_spatial()

    if request.param:
        spf = spf.persist()

    return spf


def test_spatial_points_frame_properties(s_points_frame):
    assert s_points_frame.spatial.x == 'x'
    assert s_points_frame.spatial.y == 'y'
    assert s_points_frame.spatial.p == 5
    assert s_points_frame.npartitions == 10
    assert s_points_frame.spatial.x_range == (0, 1)
    assert s_points_frame.spatial.y_range == (0, 2)
    assert s_points_frame.spatial.nrows == 1000

    # x_bin_edges
    np.testing.assert_array_equal(
        s_points_frame.spatial.x_bin_edges,
        np.linspace(0.0, 1.0, 2 ** 5 + 1))

    # y_bin_edges
    np.testing.assert_array_equal(
        s_points_frame.spatial.y_bin_edges,
        np.linspace(0.0, 2.0, 2 ** 5 + 1))

    # distance_divisions
    distance_divisions = s_points_frame.spatial.distance_divisions
    assert len(distance_divisions) == 10 + 1
