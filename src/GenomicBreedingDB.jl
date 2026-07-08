module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates

# # Load database credentials
# DotEnv.load!(joinpath(homedir(), ".env"))
# Load functions
include("connection.jl")
include("upload.jl")
include("delete.jl")
include("download.jl")

export dbconnect, dbinit
export check_illegal_strings, simulate, load_trial_df, validate_trials, validate_date
export add_col!, layout_info_parser!, add_measurement_dates!
export insert_names!, update_table_field_by_name!, insert_entry_relationships!, extract_traits, extract_ids, insert_phenotype_data!, load_trial_data!
export delete_names!


# export cleaunptraitnames, uploadtrialsorphenomes, updatedescription
# export checkparams, querytable #, extractmainfieldstablesandcols, addfilters!, querytrialsandphenomes, queryanalyses, df_to_io

end
