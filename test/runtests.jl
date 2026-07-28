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
simulate_genomes() |> simulate_trials |> simulate_environments
simulate_phenomes(readdelimited(Trials, fname = "simulated_trials.tsv"))
simulate_fit(readjld2(Genomes, fname = "simulated_genomes.jld2"), readjld2(Phenomes, fname = "simulated_phenomes.jld2"))
upload(
    "simulated_trials.tsv",
    species = "Zea mays",
    experiment = "simulated experiment",
    treatment = "control",
    entry_type = "family",
    population_type = "cultivar",
    relationship_type = "member_of",
    verbose = true,
)
upload("simulated_environments.tsv", experiment = "simulated experiment", treatment = "control", verbose = true)
upload(abspath("simulated_phenomes.jld2"), name = "simulated phenomes", notes = "simulated", verbose = true)
upload(
    abspath("simulated_reference_genome.fa"),
    name = "simulated reference genome",
    notes = "simulated",
    verbose = true,
)
upload(
    abspath("simulated_genomes.vcf"),
    name = "simulated genomes",
    notes = "simulated",
    fname_reference_genome = abspath("simulated_reference_genome.fa"),
    verbose = true,
)
upload(
    abspath("simulated_genomes.jld2"),
    name = "simulated genomes",
    notes = "simulated",
    fname_reference_genome = abspath("simulated_reference_genome.fa"),
    verbose = true,
)

Documenter.doctest(GenomicBreedingDB)

@testset "GenomicBreedingDB.jl" begin
    # Write your tests here.
    @test true
end
