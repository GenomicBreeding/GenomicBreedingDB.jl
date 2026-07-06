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
export simulate, validate_trials, add_col!, validate_date, layout_info_parser!, add_measurement_dates!
export insert_names!, update_table_field_by_name!, insert_entry_relationships!, extract_traits, extract_ids
export delete_names!


# export cleaunptraitnames, uploadtrialsorphenomes, updatedescription
# export checkparams, querytable #, extractmainfieldstablesandcols, addfilters!, querytrialsandphenomes, queryanalyses, df_to_io

end
