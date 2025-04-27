# GenomicBreedingDB

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/dev/)
[![Build Status](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml?query=branch%3Amain)


## Example PostgreSQL setup

### 1. Install PostgreSQL within a conda environment and start the server

```shell
# conda install anaconda::postgresql
# pg_ctl -D $CONDA_PREFIX/pgsql_data start
# psql postgres
wget https://ftp.postgresql.org/pub/source/v17.4/postgresql-17.4.tar.gz
wget https://ftp.postgresql.org/pub/source/v17.4/postgresql-17.4.tar.gz.sha256
x=$(cat postgresql-17.4.tar.gz.sha256 | cut -f1 -d' ')
y=$(sha256sum postgresql-17.4.tar.gz | cut -f1 -d' ')
if [ $x = $y ]
then
    echo "OK"
fi
tar -xzvf postgresql-17.4.tar.gz
rm postgresql-17.4.tar.gz
cd postgresql-17.4/
# Create a new conda environment, install dependencies and build PostgreSQL from source
# conda create -n postgresql
# conda activate postgresql
# Or install directly into GenomicBreeding conda environment
conda install -c conda-forge make icu bison flex openssl perl-lib
./configure --without-icu --with-openssl --prefix=$CONDA_PREFIX # OpenSSL is required by pgcrypto
make world-bin
make install-world-bin
# Initialise the database cluster
initdb -D $CONDA_PREFIX/pgsql_data
# Start the server
touch $CONDA_PREFIX/pgsql_data/logfile.txt
pg_ctl -D $CONDA_PREFIX/pgsql_data -l $CONDA_PREFIX/pgsql_data/logfile.txt start
# bat $CONDA_PREFIX/pgsql_data/logfile.txt --wrap never
# pg_ctl -D $CONDA_PREFIX/pgsql_data stop
# rm $CONDA_PREFIX/pgsql_data/logfile.txt
psql postgres
# Clean-up
cd ..
rm -R postgresql-17.4*
# # On Debian-based systems
# sudo apt install postgresql postgresql-common postgresql-contrib
# sudo systemctl start postgresql.service
# # sudo nano /etc/postgresql/*/main/postgresql.conf # --> set: `listen_addresses = '*'` and `port = 5432`
# sudo systemctl restart postgresql.service
# sudo -u postgres psql
# # MISC
# # sudo systemctl start postgresql
# # sudo systemctl enable postgresql
# # sudo ufw allow 5432/tcp
# # sudo -u postgres psql
# # sudo systemctl start postgresql.service
# # sudo systemctl restart postgresql.service
# # sudo -i -u postgres
# # initdb -D ${HOME}/db
# # pg_ctl -D ${HOME}/db -l logfile start &
# # pg_ctl -D ${HOME}/db status
```

### 2. Instantiate the database

Open the PostgreSQL shell:

```shell
psql postgres
```

Create a new database:

```sql
CREATE DATABASE gbdb;
\l
\c gbdb
\dt
CREATE USER jeff WITH PASSWORD 'qwerty12345';
GRANT ALL PRIVILEGES ON SCHEMA public TO jeff;
GRANT ALL PRIVILEGES ON DATABASE gbdb TO jeff;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO other_user;
\q
```

### 3. Define the login credentials

```shell
# Save as ~/.env
DB_USER="himynamejeff"
DB_PASSWORD="qwerty12345"
DB_NAME="gbdb"
DB_HOST="localhost"
# ls -lhtr $CONDA_PREFIX/pgsql_data/
# cat $CONDA_PREFIX/pgsql_data/pg_hba.conf
```

## Start the databse

```shell
conda activate GenomicBreeding
# conda env export > GenomicBreeding_conda.yaml
# pg_ctl stop
pg_ctl -D $CONDA_PREFIX/pgsql_data -l $CONDA_PREFIX/pgsql_data/logfile.txt start
```

## Use in Julia

``julia
using GenomicBreedingDB
using DotEnv
DotEnv.load!(joinpath(homedir(), ".env"))
conn = dbconnect()
querytable("entries")
close(conn)
```