using GenomicBreedingDB
using Documenter

DocMeta.setdocmeta!(GenomicBreedingDB, :DocTestSetup, :(using GenomicBreedingDB); recursive=true)

makedocs(;
    modules=[GenomicBreedingDB],
    authors="jeffersonparil@gmail.com",
    sitename="GenomicBreedingDB.jl",
    format=Documenter.HTML(;
        canonical="https://GenomicBreeding.github.io/GenomicBreedingDB.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/GenomicBreeding/GenomicBreedingDB.jl",
    devbranch="main",
)
