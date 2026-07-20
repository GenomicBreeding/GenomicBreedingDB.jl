using GenomicBreedingDB
using Test, Documenter
using GenomicBreedingCore, GenomicBreedingIO
using DotEnv, LibPQ, DataFrames, Tables, StatsBase, CSV
using Suppressor, ProgressMeter
using Random, Dates

try
    try
        dbinit();
    catch
        ;
        nothing;
    end
    fname_trial = simulate_trial()
    fname_environment = simulate_environment(fname_trial)
    conn = dbconnect()
    upload_trial_data!(
        conn,
        fname = fname_trial,
        species = "Acacia neglecta",
        experiment = "some-exp",
        treatment = "some_trt",
        entry_type = "family",
        population_type = "population",
        relationship_type = "member_of",
    );
    upload_environment_data!(conn, fname = fname_environment, experiment = "exp-1", treatment = "trt-42")
    rm.([fname_trial, fname_environment])
    close(conn)
catch
    nothing
end

Documenter.doctest(GenomicBreedingDB)

@testset "GenomicBreedingDB.jl" begin
    # Write your tests here.
    @test true
end
