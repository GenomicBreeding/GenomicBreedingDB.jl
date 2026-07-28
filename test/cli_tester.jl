using Pkg
using JuliaFormatter
Pkg.activate(".")
Pkg.update()
# Format
format("src/", margin = 120)
# Test
include("runtests.jl")
