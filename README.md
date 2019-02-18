# RethinkDB Metabase Driver

This is a non-functional WIP. Hit up @jordanh if you want to collaborate.

## Building the driver 

### Prereq: Install Metabase as a local maven dependency, compiled for building drivers

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

```bash
cd /path/to/metabase_source
lein install-for-building-drivers
```

### Build the driver

```bash
# (In the directory where you cloned this repository)
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

### Copy it to your plugins dir and restart Metabase
```bash
mkdir -p /path/to/metabase/plugins/
cp target/uberjar/sudoku.metabase-driver.jar /path/to/metabase/plugins/
jar -jar /path/to/metabase/metabase.jar
```

*or:*

```bash
mkdir -p /path/to/metabase_source/plugins
cp target/uberjar/sudoku.metabase-driver.jar /path/to/metabase_source/plugins/
cd /path/to/metabase_source
lein run
```