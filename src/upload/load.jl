"""
    simulate(; fname_output::String = "simulated_trial_data.tsv", 
             additional_params::Union{Nothing, Dict{String, String}} = nothing,
             sparsity::Float64 = 0.05,
             overwrite::Bool = true,
             verbose::Bool = false)::String

Simulate genomic breeding trial data and write it to a file.
(Note Trials struct details: https://genomicbreeding.github.io/GenomicBreedingCore.jl/stable/#GenomicBreedingCore.Trials)

This function generates simulated genomes and trial data (with or without missing data), optionally enriching the output
with additional parameters before writing to disk.

# Arguments
- `fname_output::String`: Path to the output file where simulated trial data will be written. 
  Defaults to `"simulated_trial_data.tsv"`.
- `additional_params::Union{Nothing, Dict{String, String}}`: Optional dictionary of additional 
  columns to append to the trial data (e.g., species, experiment ID, treatment). Each key-value 
  pair will be added as a column with the value repeated for all rows. Keys and values are validated
  using `check_illegal_strings()` to ensure they contain only allowed characters. Defaults to `nothing`.
- `sparsity::Float64`: Fraction of missing values in the simulated trial data. Defaults to `0.05`.
- `overwrite::Bool`: If `true`, removes the output file if it already exists before writing. 
  Defaults to `true`.
- `verbose::Bool`: If `true`, enables verbose output during simulation. Defaults to `false`.

# Returns
- `String`: The path to the output file (`fname_output`).

# Throws
- `String`: If `additional_params` contains illegal characters or non-ASCII content in column names or values.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_no_missing = simulate(fname_output="test_no_missing.tsv", sparsity=0.0);

julia> fname_with_missing = simulate(fname_output="test_with_missing.tsv", sparsity=0.1);

julia> df_no_missing = CSV.read(fname_no_missing, DataFrame, missingstring="NA");

julia> df_with_missing = CSV.read(fname_with_missing, DataFrame, missingstring="NA");

julia> rm.([fname_no_missing, fname_with_missing]);

julia> sum(Matrix(ismissing.(df_no_missing))) == 0
true

julia> mean(Matrix(ismissing.(df_with_missing))) > 0.0
true
```
"""
function simulate(;
    fname_output::String = "simulated_trial_data.tsv",
    additional_params::Union{Nothing,Dict{String,String}} = nothing,
    sparsity::Float64 = 0.05,
    overwrite::Bool = true,
    verbose::Bool = false,
)::String
    # fname_output::String = "simulated_trial_data.tsv"; overwrite::Bool = true; verbose::Bool = false; additional_params::Union{Nothing, Dict{String, String}} = nothing; sparsity=0.05
    # additional_params::Union{Nothing, Dict{String, String}} = Dict("species" => "Lolium multiflorum", "experiments" => "STR_trial-2026", "treatments" => "control")
    genomes = GenomicBreedingCore.simulategenomes(verbose = verbose)
    (trials, _) = GenomicBreedingCore.simulatetrials(
        genomes = genomes,
        sparsity = sparsity,
        verbose = verbose,
    )
    if overwrite && isfile(fname_output)
        rm(fname_output)
    end
    if isnothing(additional_params)
        GenomicBreedingIO.writedelimited(trials, fname = fname_output)
    else
        df = tabularise(trials)
        for (k, v) in additional_params
            try
                check_illegal_strings([k, v])
            catch e
                new_error = join([
                    "Illegal string/s in the requested new column [name=$k, value=$v]!\n",
                    sprint(showerror, e),
                ])
                error(new_error)
            end
            df[!, k] .= v
        end
        CSV.write(fname_output, df; delim = '\t')
    end
    fname_output
end

"""
    load_trial_df(fname::String; missingstring::Vector{String}=[])::DataFrame

Load trial data from a delimited file and return it as a DataFrame.

# Arguments
- `fname::String`: Path to the input file containing trial data.
- `missingstring::Vector{String}=[]`: Vector of strings to be interpreted as missing values. 
  If empty, attempts to use `GenomicBreedingIO.readdelimited()` with default missing strings 
  `["missing", "NA", "na", "N/A", "n/a", ""]`. Falls back to `CSV.read()` if that fails.

# Returns
- `DataFrame`: A tabularised DataFrame containing the trial data with standardised column names.

# Details
The function attempts to load trial data in the following order:
1. If `missingstring` is empty, tries to read using `GenomicBreedingIO.readdelimited()` with default 
   missing value specifications and applies `tabularise()`.
2. On failure, falls back to `CSV.read()` with default missing strings.
3. If `missingstring` is provided, directly uses `CSV.read()` with the specified missing value strings.

Column names are standardised by renaming `"#years"` to `"years"` if present.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname);

julia> rm(fname);

julia> size(df)
(12800, 14)
```
"""
function load_trial_df(fname::String; missing_strings::Vector{String} = String[])::DataFrame
    if length(missing_strings) == 0
        try
            trials = GenomicBreedingIO.readdelimited(Trials, fname = fname, sep = "\t")
            return GenomicBreedingCore.tabularise(trials)
        catch
            df = CSV.read(
                fname,
                DataFrame,
                missingstring = ["missing", "NA", "na", "N/A", "n/a", ""],
            ) # same as the missing strings in `GenomicBreedingIO.readdelimited(Trials, ...)`
            try
                rename!(df, "#years" => "years");
            catch
                ;
                nothing;
            end
            return df
        end
    else
        df = CSV.read(fname, DataFrame, missingstring = missing_strings)
        try
            rename!(df, "#years" => "years");
        catch
            ;
            nothing;
        end
        return df
    end
end
