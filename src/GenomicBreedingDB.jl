module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates
using CodecZlib, TranscodingStreams

# # Load database credentials
# DotEnv.load!(joinpath(homedir(), ".env"))
# Base functions
include("connection.jl")
export dbconnect, dbinit
include("filter.jl")
export Filter, hash, ==, concat_filters
include("checks.jl")
export check_illegal_strings,
    check, validate_trials, validate_date, validate_data_table, validate_filters, check_reference_genome, check_vcf
include("delete.jl")
export delete_names!
# Uploads
include("upload/simulate.jl")
export simulate_reference_genome,
    simulate_genomes, simulate_trials, simulate_environments, simulate_phenomes, simulate_fit
include("upload/load.jl")
export load_trial_df, extract_traits, load_environments_df, extract_environment_variables
include("upload/mutate.jl")
export add_col!, parse_layouts!, add_measurement_dates!
include("upload/insert.jl")
export insert_names!, insert_layouts!, insert_entry_relationships!
include("upload/update.jl")
export update_table_field_by_name!, update_table!
include("upload/upload_environments.jl")
export insert_environment_data!, upload_environment_data!
include("upload/upload_phenotypes.jl")
export insert_phenotype_data!, upload_trial_data!, upload_phenomes!
include("upload/upload_genotypes.jl")
export upload_reference_genome!, upload_genotype_vcf!, upload_genomes!
include("upload/upload_models.jl")
export upload_fit!
include("upload/define_relationship_tables.jl")
export define_relationships!
include("upload/upload.jl")
export upload
# Downloads
include("download/extract_ids_names.jl")
export extract_ids, extract_names
include("download/download.jl")
export extract_all_tables, extract_table_contents, query_table
include("download/transform.jl")
export unstack_data_table
















end
