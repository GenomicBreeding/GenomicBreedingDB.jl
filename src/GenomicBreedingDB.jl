module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates

# # Load database credentials
# DotEnv.load!(joinpath(homedir(), ".env"))
# Load functions
include("connection.jl")
export dbconnect, dbinit
include("delete.jl")
export delete_names!
include("checks.jl")
export check_illegal_strings, check, validate_trials, validate_date, validate_data_table, list_tables, extract_table
include("upload/simulate.jl")
export simulate_trial, simulate_environment
include("upload/load.jl")
export load_trial_df, extract_traits, load_environment_df, extract_environment_variables
include("upload/mutate.jl")
export add_col!, parse_layouts!, add_measurement_dates!
include("upload/upload_insert.jl")
export insert_names!, insert_layouts!, insert_entry_relationships!, insert_phenotype_data!, insert_environment_data!
include("upload/upload_update.jl")
export update_table_field_by_name!
include("upload/upload.jl")
export upload_trial_data!, upload_environment_data!
include("download/extract_ids_names.jl")
export extract_ids, extract_names
include("download/filter.jl")
export Filter, ==
include("download/download.jl")
export query_table
include("download/transform.jl")
export unstack_data_table
















end
