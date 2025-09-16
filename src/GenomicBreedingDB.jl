module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter

# # Load database credentials
# DotEnv.load!(joinpath(homedir(), ".env"))
# Load functions
include("connection.jl")
include("upload.jl")
include("download.jl")

export dbconnect, dbinit
export uploadtrialsorphenomes, updatedescription
export checkparams, querytable, extractmainfieldstablesandcols, addfilters!, cleaunptraitnames, querytrialsandphenomes, queryanalyses, df_to_io


end
