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
julia> fname = simulate_trial(fname_output="test.tsv");

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
