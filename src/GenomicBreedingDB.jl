module GenomicBreedingDB

using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase
using Suppressor, ProgressMeter

# Load database credentials
DotEnv.load!(joinpath(homedir(), ".env"))
# Load functions
include("connection.jl")
include("upload.jl")
include("download.jl")



end
