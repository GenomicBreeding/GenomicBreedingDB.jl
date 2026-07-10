using GenomicBreedingDB
using Test, Documenter
using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates

try
    dbinit()
catch
    nothing
end

Documenter.doctest(GenomicBreedingDB)

@testset "GenomicBreedingDB.jl" begin
    # Write your tests here.
    @test true
end
