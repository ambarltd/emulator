# Ambar Emulator

A local version of Ambar for testing and initial integration.

Connect your databases to multiple consumers with minimal configuration and no libraries needed.

## Configuration

A YAML configuration file should describe all sources and destinations to be used.

``` yaml
# Connections to your databases.
# The Emulator will read data from those dbs.
data_sources:
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
      - postgres source
      - file source

  # Send data to a file. One entry per line.
  - id: file_destination
    description: my projection 1
    type: file
    path: ./temp.file

    sources:
      - postgres_source
      - file_source
```

## Running the program

You only need to provide the address of the configuration file and the emulator
will start streaming your data.

``` bash
> emulator run --config config.yaml
```

## Help

You can see all commands available with the `--help` flag

``` bash
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
