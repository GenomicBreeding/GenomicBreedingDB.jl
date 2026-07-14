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
include("upload/checks.jl")
include("upload/simulate.jl")
include("upload/load.jl")
include("upload/mutate.jl")
include("upload/upload.jl")
include("download/extract_ids.jl")
include("download/download.jl")

export dbconnect, dbinit
export delete_names!
export check_illegal_strings
export simulate_trial, simulate_environment
export load_trial_df, extract_traits, extract_environment_variables, load_environment_df
export validate_trials, validate_date
export add_col!, parse_layouts!, add_measurement_dates!
export insert_names!, update_table_field_by_name!, insert_layouts!, insert_entry_relationships!
export insert_phenotype_data!, upload_trial_data!
export insert_environment_data!, upload_environment_data!
export extract_ids

# export cleaunptraitnames, uploadtrialsorphenomes, updatedescription
# export checkparams, querytable #, extractmainfieldstablesandcols, addfilters!, querytrialsandphenomes, queryanalyses, df_to_io

end
