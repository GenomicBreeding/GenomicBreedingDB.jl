# GenomicBreedingDB

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/dev/)
[![Build Status](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml?query=branch%3Amain)


## Example PostgreSQL setup

### 1. Install PostgreSQL with pixi, and initialise the server

```shell
cd GenomicBreedingDB.jl/
pixi init
pixi add postgresql
pixi run initdb -D ./pgsql_data
pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt start
pixi run psql postgres
```

### 2. Instantiate the database

Open the PostgreSQL shell:

```shell
cd GenomicBreedingDB.jl/
pixi run psql postgres
```

Create a new database:

```sql
CREATE DATABASE gbdb;
\l
\c gbdb
\dt
CREATE USER himynamejeff WITH PASSWORD 'qwerty12345';
GRANT ALL PRIVILEGES ON SCHEMA public TO himynamejeff;
GRANT ALL PRIVILEGES ON DATABASE gbdb TO himynamejeff;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO other_user;
-- \c postgres
-- DROP DATABASE gbdb;
-- \l
-- SELECT * FROM pg_database;
\q
```

### 3. Define the login credentials

```shell
cat > ~/.env << 'EOF'
DB_USER="himynamejeff"
DB_PASSWORD="qwerty12345"
DB_NAME="gbdb"
DB_HOST="localhost"
EOF
```

### 4. Initiliase the tables

#### Start the database

```shell
cd GenomicBreedingDB.jl/
pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt start
# pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt stop
# pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt restart
```

#### Initialise the tables

```shell
cd GenomicBreedingDB.jl/
julia --project=. --threads=2,1 --load test/interactive_prelude.jl
```

```julia
dbinit()
# Test
conn = dbconnect()
close(conn)
# Initialise the database
dbinit()
# Test query
querytable("entries")
```

## Dev stuff:

### REPL prelude

```shell
julia --project=. --threads=2 --load test/interactive_prelude.jl
```

### Format and test

```shell
time julia --project=. --threads=2 -e "using Pkg; Pkg.update()"
time julia --project=. --threads=2  test/cli_tester.jl
```
