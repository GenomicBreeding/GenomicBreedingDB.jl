module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates

# # Load database credentials
# DotEnv.load!(joinpath(homedir(), ".env"))
# Load functions
include("connection.jl")
include("delete.jl")
include("checks.jl")
include("upload/simulate.jl")
include("upload/load.jl")
include("upload/mutate.jl")
include("upload/upload_insert.jl")
include("upload/upload_update.jl")
include("upload/upload.jl")
include("download/extract_ids_names.jl")
include("download/download.jl")

export dbconnect, dbinit
export delete_names!
export check_illegal_strings, validate_trials, validate_date
export simulate_trial, simulate_environment
export load_trial_df, extract_traits, load_environment_df, extract_environment_variables
export add_col!, parse_layouts!, add_measurement_dates!
export insert_names!, insert_layouts!, insert_entry_relationships!, insert_phenotype_data!, insert_environment_data!
export update_table_field_by_name!
export upload_trial_data!, upload_environment_data!
export extract_ids, extract_names


end
