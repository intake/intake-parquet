Quickstart
==========

``intake_parquet`` provides quick and easy access to tabular data stored in
the Apache `Parquet`_ binary, columnar format.

.. _Parquet: https://parquet.apache.org/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c conda-forge intake-parquet

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_parquet`` will become available. This
can be used to open any parquet data-set. For example, assuming the part ``'mydata.parquet'``
contains parquet data in one or more files, the following will load it into an in-memory pandas
dataframe::

   import intake
   source = intake.open_parquet('mydata.parquet')
   dataframe = source.read()

Arguments to ``open_parquet``:

- ``urlpath`` : the location of the data. This can be a single file, a list of specific files,
 or a directory containing parquet files (normally containing a ``_metadata`` index file). The
 URLs can be local files or, if using a protocol specifier such as ``'s3://'``, a remote file
 location.

- ``storage_options`` : other parameters that are to be passed to the  file-system
 implementation, in the case that a remote file-system is referenced in ``urlpath``. For
 specifics, see the dask `documentation`_.

- ``columns`` : Of the possible set of columns stored in the data, only those specified
 will be read; other data are not accessed at all. If not specified, loads all columns.

- ``index`` : Set the given column as the index of the resultant data-frame. If not given,
 a default index may be set, if the information is available in the metadata of the data-set;
 or no index if not. Can be set to ``False`` to prevent setting an index.

- ``filters`` : A list of filters to consider excluding the loading of some of the partitions
 of the data. For example, if there is a column called ``'value'``, a filter like
 ``('value', '>', 5)``, then partitions which contain *no values* matching the filter will not
 be loaded, but partitions containing *at least one* value which passes the filter will be
 loaded.

- ``engine`` : 'fastparquet' or 'pyarrow'. Which backend to read with.

- ``gather_statistics`` : bool or None (default).  Gather the statistics for
  each dataset partition. By default, this will only be done if the _metadata
  file is available. Otherwise, statistics will only be gathered if True,
  because the footer of every file will be parsed (which is very slow on some
  systems).

- see ``dd.read_parquet()`` for the other named parameters that can be passed through.

.. _documentation : http://dask.pydata.org/en/latest/remote-data-services.html

A source so defined will provide the usual methods such as ``discover`` and ``read_partition``.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, entries must specify ``driver: parquet``.
The further arguments are exactly the same
as for ``open_parquet``. Commonly, the choice of which columns to load can be left to the
end-user, by including it as a parameter.

Using a Catalog
~~~~~~~~~~~~~~~

Assuming a catalog file called ``cat.yaml``, containing a parquet source ``pdata``, one could
load it into a dataframe as follows::

   import intake
   cat = intake.open_catalog('cat.yaml')
   df = cat.pdata.read()

Parquet data-sets are inherently partitioned, and the partitions can be accessed in random order
or iterated over.

Parquet data also plays well with Dask parallel processing, so the method ``to_dask()`` can
be considered. Importantly, sub-selecting from the columns of the Dask data-frame will prevent
unnecessary loading of the non-required columns even in the case where columns selection has
not been included in the catalog entry user parameters.

Caching
~~~~~~~

Parquet data-sets can be singular, lists of files, or whole directory trees. The first two can
be cached using the standard "files" type cache, but the latter requires "dir" type cachimg to
capture the whole structure. An example may look like:

.. code-block:: yaml

    cache:
      - type: dir
        regex: '{{ CATALOG_DIR }}/split'
        argkey: urlpath
        depth: 4

Where the extra ``depth`` parameter indicates the number of directory levels that should be
scanned.
