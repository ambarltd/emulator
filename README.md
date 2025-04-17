# Ambar Emulator

A local version of Ambar for testing and initial integration.

Connect your databases to multiple consumers with minimal configuration and no libraries needed.

Currently supported databases:
- PostgreSQL
- MySQL

## Configuration

A YAML configuration file should describe all sources and destinations to be used.

``` yaml
# Connections to your databases.
# The Emulator will read data from those dbs.
data_sources:

  # Connect to a PostgreSQL database
  - id: postgres_source
    description: Main events store
    type: postgres
    host: localhost
    port: 5432
    username: my_user
    password: my_pass
    database: my_db
    table: events_table
    columns:
      - id
      - aggregate_id
      - sequence_number
      - payload
    serialColumn: id
    partitioningColumn: aggregate_id
    pollingInterval: 0.050

  # Connect to a MySQL database
  - id: mysql_source
    description: Main events store
    type: mysql
    host: localhost
    port: 5432
    username: my_user
    password: my_pass
    database: my_db
    table: events_table
    columns:
      - id
      - aggregate_id
      - sequence_number
      - payload
    autoIncrementingColumn: id
    partitioningColumn: aggregate_id
    pollingInterval: 0.050

  # Connect to an SQLServer database
  - id: sqlserver_source
    description: Main events store
    type: sqlserver
    host: localhost
    port: 1433
    username: my_user
    password: my_pass
    database: my_db
    table: events_table
    columns:
      - id
      - aggregate_id
      - sequence_number
      - payload
    autoIncrementingColumn: id
    partitioningColumn: aggregate_id
    pollingInterval: 0.050

  # Use a plain text file as a data source.
  # Each line must be a valid JSON object.
  # Values are projected as they are added.
  - id: file_source
    description: My file JSON event store
    type: file
    path: ./path/to/source.file
    incrementingField: id
    partitioningField: aggregate_id
    pollingInterval: 0.050

# Connections to your endpoint.
# The Emulator will send data read from the databases to these endpoints.
data_destinations:

  # Send data via HTTP
  - id: http_destination
    description: my projection 2
    type: http-push
    endpoint: http://some.url.com:8080/my_projection
    username: name-of-user
    password: password123

    sources:
      - postgres_source
      - sqlserver_source
      - file_source

  # Send data to a file. One entry per line.
  - id: file_destination
    description: my projection 1
    type: file
    path: ./temp.file

    sources:
      - postgres_source
      - sqlserver_source
      - file_source
```

## Projections info

The emulator starts a server that provides information about projections progress on the `/projections` endpoint.

## Running the program

You only need to provide the address of the configuration file and the emulator
will start streaming your data.

``` bash
> emulator run --config config.yaml
```

## Help

You can see all commands available with the `--help` flag

``` bash
$ emulator --help

Ambar Emulator v0.0.1 - alpha release

  A local version of Ambar <https://ambar.cloud>
  Connect your databases to multiple consumers with minimal configuration and no libraries needed.

Usage: emulator COMMAND

Available options:
  --version                Show version information
  -h,--help                Show this help text

Available commands:
  run                      run the emulator

More info at <https://github.com/ambarltd/emulator>
```

And these are the options for the `run` command.

```
$ emulator run --help

Usage: emulator run [--partitions-per-topic INT] [--port INT]
                    [--override-polling-interval SECONDS] [--data-path PATH]
                    --config FILE [--verbose]

  run the emulator

Available options:
  -h,--help                Show this help text
  --partitions-per-topic INT
                           How many partitions should newly created topics have.
  --port INT               Port to attach projections info server to.
                           (default: 8080)
  --override-polling-interval SECONDS
                           Override the polling interval for all polled data
                           sources.
  --data-path PATH         Where to put emulation data including file queues.
                           Defaults to $XDG_DATA_HOME/ambar-emulator.
  --config FILE            Yaml file with environment configuration. Spec at at
                           <https://github.com/ambarltd/emulator>.
  --verbose                Enable verbose logging.
```
