```@meta
CurrentModule = GenomicBreedingDB
```

# GenomicBreedingDB

Documentation for [GenomicBreedingDB](https://github.com/GenomicBreeding/GenomicBreedingDB.jl).

A PostgreSQL-backed database layer for the GenomicBreeding ecosystem.

`GenomicBreedingDB.jl` provides tools for managing genomic, phenomic, environmental, experimental, and model data in a relational database.

It integrates with:

- GenomicBreedingCore.jl
- GenomicBreedingIO.jl
- LibPQ.jl
- DataFrames.jl

The package supports:

- Database creation and validation
- Trial and environmental data ingestion
- Genomic and phenomic dataset registration
- Reference genome and VCF management
- Relationship tracking between datasets and metadata
- Flexible filtering and querying
- Download and transformation utilities
- Simulation of example datasets


# Installation

```julia
using Pkg
Pkg.add("GenomicBreedingDB")
```

Database connectivity is configured through environment variables stored in:

```text
~/.env
```

Example:

```bash
DB_NAME=my_database
DB_USER=my_user
DB_PASSWORD=my_password
DB_HOST=localhost
```


# Quick Start

## Connect to a Database

```julia
using GenomicBreedingDB

conn = dbconnect()
```

## Initialise the Schema

```julia
dbinit()
```

## Upload Trial Data

```julia
upload(
    "trial_data.tsv",
    species = "Acacia neglecta",
    experiment = "trial_001",
    treatment = "control",
    entry_type = "family",
    population_type = "population",
    relationship_type = "member_of"
)
```

## Query Data

```julia
conn = dbconnect()

filters = [
    Filter(
        conn,
        table = "phenotype_data",
        field = "trait",
        filter_like = "height"
    )
]

df = query(conn, filters)
```


# Database Workflow

A typical workflow consists of:

```text
Raw files
    │
    ├── Trials (.tsv)
    ├── Environments (.tsv)
    ├── Reference genomes (.fa)
    ├── Genotypes (.vcf)
    ├── Genomes (.jld2)
    ├── Phenomes (.jld2)
    └── Fits (.jld2)

            ↓

      upload(...)

            ↓

     PostgreSQL Database

            ↓

     query(...)
     download(...)
```


# Core Concepts

## Metadata Tables

Metadata entities are stored independently and referenced through foreign keys.

Examples include:

- Species
- Entries
- Experiments
- Sites
- Treatments
- Traits
- Measurements
- Environment variables
- Reference genomes

## Observation Tables

Phenotypic and environmental measurements are stored in normalized form.

### Phenotype Data

```text
entry
experiment
site
treatment
layout
measurement
trait
value
```

### Environment Data

```text
experiment
site
treatment
layout
measurement
environment_variable
value
```

## Dataset Registries

Large datasets are tracked through dedicated tables:

- `reference_genomes`
- `genotype_vcfs`
- `genomes`
- `phenomes`
- `fits`

Only metadata and file locations are stored in the database.


# Main Features

## Database Management

```@docs
dbconnect
dbinit
```

## Filters and Query Construction

```@docs
Filter
concat_filters
```

## Validation Utilities

```@docs
check
check_date
check_trials
check_illegal_strings
check_reference_genome
check_vcf
check_dsv
```

## Data Loading Helpers

```@docs
load_trial_df
load_environments_df
extract_traits
extract_environment_variables
```

## DataFrame Utilities

```@docs
add_col!
parse_layouts!
add_measurement_dates!
```

## Data Insertion and Updates

```@docs
insert_names!
insert_layouts!
insert_entry_relationships!
update_table!
update_table_field_by_name!
delete_names!
```

## Environment Uploads

```@docs
insert_environment_data!
upload_environment_data!
```

## Phenotype Uploads

```@docs
insert_phenotype_data!
upload_trial_data!
upload_phenomes!
```

## Genotype Uploads

```@docs
upload_reference_genome!
upload_genotype_vcf!
upload_genomes!
```

## Model Uploads

```@docs
upload_fit!
```

## Relationship Management

```@docs
define_relationships!
```

## Universal Upload Interface

Most users can simply use:

```julia
upload("my_file")
```

which automatically detects the file type and dispatches to the correct upload routine.

```@docs
upload
```


# Querying and Downloading

The package provides a schema-aware query interface that automatically resolves foreign key identifiers into human-readable names.

## Example

```julia
conn = dbconnect()

filters = [
    Filter(
        conn,
        table = "entries",
        field = "name",
        filter_like = "family"
    )
]

df = query(conn, filters)
```

### Database Introspection

```@docs
list_all_tables
extract_table_fields
extract_table_contents
extract_ids
extract_names
```

### Queries

```@docs
query
define_filters
combinations
```

### Downloads

```@docs
download
unstack_data_table
```


# Simulated Example Data

The package provides simulation utilities for testing workflows and populating development databases.

Generate an entire example pipeline:

```julia
genomes = simulate_genomes()

trials = simulate_trials(genomes)

phenomes = simulate_phenomes(trials)

fit = simulate_fit(genomes, phenomes)

filter(x -> occursin(Regex("^simulated_"), x), readdir())
```

Available simulation functions:

```@docs
simulate_reference_genome
simulate_genomes
simulate_trials
simulate_environments
simulate_phenomes
simulate_fit
```


# Typical End-to-End Example

```julia
using GenomicBreedingDB

dbinit()

conn = dbconnect()

genomes = simulate_genomes()

simulate_trials(genomes)

upload(
    "simulated_trials.tsv",
    species = "Acacia neglecta",
    experiment = "example_trial",
    treatment = "control",
    entry_type = "family",
    population_type = "population",
    relationship_type = "member_of"
)

filters = [
    Filter(
        conn,
        table = "phenotype_data",
        field = "trait",
        filter_like = "trait_1"
    )
]

df = query(conn, filters)

close(conn)
```


# Design Principles

`GenomicBreedingDB.jl` is designed around:

- Strong validation before database writes
- Parameterised SQL queries
- Metadata normalization
- Relationship-driven dataset tracking
- Human-readable query interfaces
- Reproducible genomic breeding workflows



# Module Reference

```@autodocs
Modules = [GenomicBreedingDB]
Order   = [:type, :function]
```