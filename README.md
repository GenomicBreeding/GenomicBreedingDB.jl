# GenomicBreedingDB

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://GenomicBreeding.github.io/GenomicBreedingDB.jl/dev/)
[![Build Status](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/GenomicBreeding/GenomicBreedingDB.jl/actions/workflows/CI.yml?query=branch%3Amain)

The database schema is a **species-agnostic**, **entry-centric** breeding database designed to integrate pedigree, phenotype, genotype, and genomic prediction information within a single relational framework.

The architecture deliberately separates **data storage and management** from **computational analysis**:
- **PostgreSQL** serves as the authoritative repository for metadata and entity relationships while supporting basic data retrieval. Complex joins are largely delegated to the analytical layer to keep SQL queries simpler and more maintainable.
- **Julia** and [**GenomicBreeding**](https://github.com/GenomicBreeding) serves as the computational engine, performing complex joins and data transformations, managing large genomic and phenotypic matrices, and executing genomic prediction and statistical analyses.

This separation enables:
- efficient scaling to large genomic datasets,
- support for complex pedigree structures,
- integration with genomic prediction pipelines,
- improved reproducibility, and
- broad applicability across species and breeding programs.

## Quickstart

Assuming PostgreSQL has been setup ([see below for details](#postgresql-setup)):

### Uploads and downloads using simulated data

```julia
using GenomicBreedingDB.jl
# Simulate
genomes = simulate_genomes()
trials = simulate_trials(genomes)
phenomes = simulate_phenomes(trials)
fit = simulate_fit(genomes, phenomes)
# Uploads
upload("siumulated_trials.tsv")
upload("siumulated_environments.tsv")
upload("siumulated_reference_genome.fa")
upload("siumulated_genomes.vcf", fname_reference_genome="siumulated_reference_genome.fa")
upload("siumulated_genomes.jld2", fname_reference_genome="siumulated_reference_genome.fa")
upload("siumulated_phenomes.jld2")
upload("siumulated_fit.jld2")
# Downloads
# TODO....
```

### Input file formats

- Tab-delimited files:
    + trial data (e.g. ["simulated_trials.tsv"](./res/simulated_trials.tsv))
    + environmental data (e.g. ["simulated_environments.tsv"](./res/simulated_environments.tsv))
- FASTA ([see specifications for details](https://en.wikipedia.org/wiki/FASTA_format))
    + reference genome file (e.g. ["simulated_reference_genome.fa"](./res/simulated_reference_genome.fa))
- VCF ([see specifications for details](https://samtools.github.io/hts-specs/VCFv4.2.pdf))
    + genotype data file (e.g. ["simulated_genomes.vcf"](./res/simulated_genomes.vcf))
- JLD2
    + [Genomes struct](https://genomicbreeding.github.io/GenomicBreedingCore.jl/dev/#GenomicBreedingCore.Genomes) (e.g. ["simulated_genomes.jld2"](./res/simulated_genomes.jld2))
    + [Phenomes struct](https://genomicbreeding.github.io/GenomicBreedingCore.jl/dev/#GenomicBreedingCore.Phenomes) (e.g. ["simulated_phenomes.jld2"](./res/simulated_phenomes.jld2))
    + [Fit struct](https://genomicbreeding.github.io/GenomicBreedingCore.jl/dev/#GenomicBreedingCore.Fit) (e.g. ["simulated_fit.jld2"](./res/simulated_fit.jld2))

### Output file format

Since only the trial and environmental data are stored in the database, 
with only the file paths and corresponding metadata of the genomic, phenomic and model data are stored,
we only output tabular data and write them out as tab-delimited files.

**--- TODO ---**
**... more details ...**

## Database schema

### Core Design Principles

- Entries are the central biological entity, representing cultivars, populations, individuals, and families.
- Pedigrees are represented as a graph, allowing flexible relationships such as parentage, cloning, and population membership.
- Phenomic data are stored via star-schema design, linking entries to experiments, sites, treatments, layouts, measurement events, and traits.
- Environmental data are similarly stored via star-schema design, linking environmental variables to experiments, sites, treatments, layouts, and measurement events.
- Large genomic datasets and fitted models are stored externally as Julia/JLD2 objects, while PostgreSQL stores metadata, and relationships.
- The schema is applicable to both plant and animal breeding programs, avoiding species-specific assumptions.

### Assumptions

- Entry names are globally unique across all species.
- Phenotype data are represented as numeric values, with categorical traits encoded numerically.
- Biological validation rules (e.g. pedigree consistency) are primarily enforced in the database (i.e. an entry cannot be its own parent).
- Trait units are embedded within trait names (e.g. `yield_t_ha`, `height_cm`) instead of being managed through a separate units system.
- Pedigree structures may be incomplete, complex, or non-traditional, and stored as flexible relationships rather than fixed maternal/paternal columns.

### Data Types

#### Biological Entities
- Species
- Entries (`cultivar`, `population`, `individual`, `family`)
- Pedigree and membership relationships (`member_of`, `clone_of`, `parent_is`, `maternal_parent_is`, `paternal_parent_is`)

#### Experimental Metadata
- Experiments
- Sites
- Treatments
- Layouts (field or facility layout)
- Measurement events

#### Phenotypes
- Trait definitions
- Numeric phenotype observations
- Full experimental context for every observation

#### Genomics
- Reference genomes
- VCF datasets
- Genome objects (JLD2)
- Phenome objects (JLD2)
- Genomic prediction model objects (JLD2)

#### Relationships
- Explicit links between genome, phenome, fit, entry, trait, experiment, site, treatment, and measurement datasets.
- Supports reproducibility, traceability, and dataset lineage tracking.

#### Schema graph

![](./db/graph.svg)

## PostgreSQL setup

### 1. Install PostgreSQL with pixi, and initialise the server

```shell
cd GenomicBreedingDB.jl/
pixi init
pixi add postgresql
pixi run initdb -D ./pgsql_data
pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt start
pixi run psql postgres
# # Or using a specific port:
# PORT=5433
# pixi run pg_ctl -D ~/pgsql_data -l ~/pgsql_data/logfile.txt -o "-p $PORT" start
# pixi run psql -h localhost -p $PORT postgres
# pixi run createuser --interactive --pwprompt
# #!/bin/bash
# createuser \
#   --pwprompt \
#   --no-superuser \
#   --no-createdb \
#   --no-createrole \
#   "$@"
```

### 2. Instantiate the database

Open the PostgreSQL shell:

```shell
cd GenomicBreedingDB.jl/
pixi run psql postgres
```

Create a new database:

```sql
\l
CREATE USER himynamejeff WITH PASSWORD 'qwerty12345';
-- DROP DATABASE gbdb;
CREATE DATABASE gbdb OWNER himynamejeff;
\c gbdb
\dt
-- GRANT ALL PRIVILEGES ON SCHEMA public TO himynamejeff;
-- GRANT ALL PRIVILEGES ON DATABASE gbdb TO himynamejeff;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO other_user;
-- \c postgres
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

### 4. Initialise the tables

#### Start the database

```shell
cd GenomicBreedingDB.jl/
pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt start
# pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt stop
# pixi run pg_ctl -D ./pgsql_data -l ./pgsql_data/logfile.txt restart
```

#### Initialise the tables

1. Open julia, initialise GenomicBreedingDB.jl, and start an interactive Julia session:

```shell
cd GenomicBreedingDB.jl/
pixi run julia --project=. --threads=2,1 -e "using Pkg; Pkg.instantiate()"
pixi run julia --project=. --threads=2,1 --load test/interactive_prelude.jl
```

2. Initialise tables using the `./db/schema.sql`:

```julia
# Initialise the database
dbinit()
# Open a connection to the database, list all the tables initialised, and close the connection
conn = dbconnect()
list_all_tables(conn)
close(conn)
```

## Database schema visualisation

```shell
cd GenomicBreedingDB.jl
pixi run eralchemy -i postgresql://himynamejeff@localhost:5432/gbdb -o db/graph.svg
```

## PostgreSQL backup/archiving setup

### (1/5) `config.sh`

```shell
#!/usr/bin/env bash

# PostgreSQL

export PGHOST="localhost"
export PGPORT="5432"
export PGDATABASE="breeding_db"
export PGUSER="backup_user"

# Storage locations

export BACKUP_ROOT="/pg_backups"
export BASE_BACKUP_DIR="${BACKUP_ROOT}/base"
export WAL_ARCHIVE_DIR="/pg_archive"

# Restore

export PGDATA="/var/lib/postgresql/17/main"

# Retention

export KEEP_WEEKLY_DAYS=365
export KEEP_WAL_DAYS=90

# Logging

export LOG_DIR="${BACKUP_ROOT}/logs"

mkdir -p "$BASE_BACKUP_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$WAL_ARCHIVE_DIR"

```

### (2/5) `backup-base.sh`

```shell
#!/usr/bin/env bash

set -euo pipefail

source ./config.sh

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

BACKUP_DIR="${BASE_BACKUP_DIR}/${TIMESTAMP}"

echo "Creating backup: ${BACKUP_DIR}"

pg_basebackup \
    -U "$PGUSER" \
    -D "$BACKUP_DIR" \
    -Ft \
    -z \
    -Xs \
    -P

echo "Backup completed."

echo "$TIMESTAMP" > "${BACKUP_DIR}/backup.timestamp"

cat <<EOF > "${BACKUP_DIR}/metadata.txt"
Database: ${PGDATABASE}
Backup Time: $(date)
Host: $(hostname)
EOF

# crontab -e 0 2 * * 0 /opt/postgres-backup/backup-base.sh

# ##############################
# ### MISC: list all backups
# source ./config.sh
# echo "================"
# echo "Base Backups"
# echo "============"
# ls -lh "$BASE_BACKUP_DIR"
# echo "================"
# echo "Latest WAL files"
# echo "================"
# ls -ltr "$WAL_ARCHIVE_DIR" | tail -20
```

### (3/5) `cleanup.sh`

```shell
#!/usr/bin/env bash

set -euo pipefail

source ./config.sh

echo "Removing old base backups..."

find "$BASE_BACKUP_DIR" \
    -mindepth 1 \
    -maxdepth 1 \
    -type d \
    -mtime +${KEEP_WEEKLY_DAYS} \
    -exec rm -rf {} \;

echo "Removing old WAL files..."

find "$WAL_ARCHIVE_DIR" \
    -type f \
    -mtime +${KEEP_WAL_DAYS} \
    -delete

echo "Cleanup complete."

# crontab -e 0 3 1 * * /opt/postgres-backup/cleanup.sh
```

### (4/5) `create-restore-point.sh`

```shell
#!/usr/bin/env bash

set -euo pipefail

LABEL="${1}"

if [[ -z "$LABEL" ]]; then
    echo "Usage:"
    echo "create-restore-point.sh label"
    exit 1
fi

psql <<EOF
SELECT pg_create_restore_point('${LABEL}');
EOF

echo "Restore point created:"
echo "$LABEL"

# ./create-restore-point.sh before_genotype_import_20260724
```

### (5/5) `restore-pitr.sh`

```shell
#!/usr/bin/env bash

set -euo pipefail

source ./config.sh

TARGET_TIME="$1"

if [[ -z "${TARGET_TIME}" ]]; then
    echo "Usage:"
    echo "restore-pitr.sh 'YYYY-MM-DD HH:MM:SS'"
    exit 1
fi

LATEST_BACKUP=$(
    ls -dt ${BASE_BACKUP_DIR}/* |
    head -n1
)

echo "Using backup:"
echo "$LATEST_BACKUP"

###############################################################################
# SAFETY BACKUP CURRENT DATABASE
###############################################################################

SAFETY_DIR="${BACKUP_ROOT}/emergency"

mkdir -p "$SAFETY_DIR"

SAFETY_FILE="${SAFETY_DIR}/pre_restore_$(date +%Y%m%d_%H%M%S).tar.gz"

echo "Stopping PostgreSQL..."

systemctl stop postgresql

echo "Saving current live cluster..."

tar -czf \
    "$SAFETY_FILE" \
    "$PGDATA"

echo "Saved:"
echo "$SAFETY_FILE"

###############################################################################
# RESTORE BASE BACKUP
###############################################################################

rm -rf "$PGDATA"

mkdir -p "$PGDATA"

echo "Locating backup archive..."

BASE_TAR=$(find "$LATEST_BACKUP" -name "*.tar.gz" | head -n1)

tar -xzf "$BASE_TAR" \
    -C "$PGDATA"

chown -R postgres:postgres "$PGDATA"

###############################################################################
# CONFIGURE PITR
###############################################################################

touch "${PGDATA}/recovery.signal"

cat >> "${PGDATA}/postgresql.auto.conf" <<EOF

restore_command = 'cp ${WAL_ARCHIVE_DIR}/%f %p'
recovery_target_time = '${TARGET_TIME}'
recovery_target_action = 'promote'

EOF

###############################################################################
# START RECOVERY
###############################################################################

echo "Starting PostgreSQL..."

systemctl start postgresql

echo
echo "Recovery initiated."
echo "Target timestamp:"
echo "$TARGET_TIME"

# sudo ./restore-pitr.sh "2026-07-24 16:02:13"
```

## Dev stuff:

### REPL prelude

```shell
julia --project=. -e 'using Pkg; Pkg.instantiate()' # For a fresh Julia installation
julia --project=. --threads=2 --load test/interactive_prelude.jl
```

### Format and test

```shell
time julia --project=. --threads=2 -e "using Pkg; Pkg.update()"
time julia --project=. --threads=2  test/cli_tester.jl
```
