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
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> prod(size(df)) > 0
true
```
"""
function load_trial_df(fname::String; missing_strings::Vector{String} = String[])::DataFrame
    if !isfile(fname)
        error("The trial data file: \"$fname\" does not exist!")
    end
    if length(missing_strings) == 0
        try
            trials = GenomicBreedingIO.readdelimited(Trials, fname = fname, sep = "\t")
            return GenomicBreedingCore.tabularise(trials)
        catch
            df = CSV.read(fname, DataFrame, missingstring = ["missing", "NA", "na", "N/A", "n/a", ""]) # same as the missing strings in `GenomicBreedingIO.readdelimited(Trials, ...)`
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

"""
    extract_traits(df::DataFrame; verbose::Bool=false)::Vector{String}

Extract trait column names from a DataFrame by filtering out trial metadata, 
ID columns, and non-numeric columns.

# Arguments
- `df::DataFrame`: Input DataFrame containing trial data and trait measurements
- `verbose::Bool=false`: If `true`, print information about found traits

# Returns
- `Vector{String}`: Vector of trait column names that contain numeric data

# Details
This function identifies trait columns by:
1. Excluding standard trial columns (fields from `Trials` type; see: https://genomicbreeding.github.io/GenomicBreedingCore.jl/stable/#GenomicBreedingCore.Trials)
2. Excluding common metadata columns (dates, species, experiments, etc.)
3. Filtering out ID columns (columns ending with `_id` or named "id")
4. Removing columns with no valid numeric values (all missing, NaN, or Inf)

# Throws
- `String`: Error message if no valid numeric trait columns are found after filtering

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> traits = extract_traits(df);

julia> traits == filter(x -> !isnothing(match(Regex("^trait_"), x)), names(df))
true
```
"""
function extract_traits(df::DataFrame; verbose::Bool = false)::Vector{String}
    trial_columns = sort(
        filter(x -> isnothing(match(Regex("phenotypes|traits"), x)), String.(string.(collect(fieldnames(Trials))))),
    )
    additional_columns = [
        "dates",
        "species",
        "experiments",
        "treatments",
        "entry_types",
        "population_types",
        "relationship_types",
        "dates",
        "years_seasons",
        "layouts",
    ]
    trait_names = setdiff(names(df), vcat(trial_columns, additional_columns))
    try
        check_illegal_strings(String.(trait_names))
    catch e
        new_error = join(["Illegal string/s in the list of \"$trait_names\"!\n", sprint(showerror, e)])
        error(new_error)
    end
    for trait in trait_names
        # trait = trait_names[1]
        # trait = "dates"
        if (trait == "id") || !isnothing(match(Regex("_id\$"), trait))
            filter!(x -> x != trait, trait_names)
            continue
        end
        y =
            df[!, trait] |>
            x ->
                filter(xi -> !ismissing(xi), x) |>
                x -> filter(xi -> try
                    !isnan(xi);
                catch
                    ;
                    false;
                end, x) |> x -> filter(xi -> try
                    !isinf(xi);
                catch
                    ;
                    false;
                end, x)
        if length(y) < 1
            filter!(x -> x != trait, trait_names)
        end
    end
    if length(trait_names) < 1
        trait_names = setdiff(names(df), vcat(trial_columns, additional_columns))
        error(
            "Found $(length(trait_names)) candidate traits but were all non-numeric: [\"$(join(trait_names, "\", \""))\"].",
        )
    end
    if verbose
        println("Found $(length(trait_names)) traits: [\"$(join(trait_names, "\", \""))\"].")
    end
    String.(trait_names)
end

"""
    load_environments_df(
        fname;
        missing_strings::Vector{String}=[
            "missing",
            "NA",
            "na",
            "N/A",
            "n/a",
            "",
        ],
    )::DataFrame

Load environmental data from a delimited text file into a DataFrame.

This function reads an environmental data file using `CSV.read()` and converts
specified strings representing missing values into `missing`.

# Arguments

- `fname`: Path to the environmental data file.

# Keyword Arguments

- `missing_strings::Vector{String}`: Strings that should be interpreted as
  missing values during import. The default values are:

  - `"missing"`
  - `"NA"`
  - `"na"`
  - `"N/A"`
  - `"n/a"`
  - `""` (empty string)

# Details

The function first verifies that the supplied file exists. If the file is
found, it is imported using `CSV.read()` with the specified missing value
strings passed to the `missingstring` argument.

The returned object is a `DataFrame` containing the imported environmental
data.

# Returns

- `DataFrame`: The contents of the input file with recognised missing-value
  strings converted to `missing`.

# Throws

- `ErrorException`: If `fname` does not correspond to an existing file.
- Any exception generated by `CSV.read()` while parsing the file.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials |> simulate_environments;

julia> df = load_environments_df("simulated_environments.tsv");

julia> prod(size(df)) > 0
true
```
"""
function load_environments_df(
    fname;
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
)::DataFrame
    if !isfile(fname)
        error("The environmental data file: \"$fname\" does not exist!")
    end
    CSV.read(fname, DataFrame, missingstring = missing_strings)
end


"""
    extract_environment_variables(
        df::DataFrame;
        verbose::Bool=false,
    )::Vector{String}

Extract environmental variable names from a DataFrame.

This function identifies candidate environmental variables by excluding
known identifier and metadata columns, validating the remaining column names,
and retaining only columns that contain at least one finite, non-missing
numeric value.

# Arguments

- `df::DataFrame`: A DataFrame containing identifier columns and environmental
  measurements.

# Keyword Arguments

- `verbose::Bool=false`: If `true`, prints a summary of the environmental
  variables that were identified.

# Details

The following columns are excluded from consideration:

- `measurements`
- `sites`
- `replications`
- `blocks`
- `rows`
- `cols`
- `experiments`
- `treatments`
- `dates`
- `layouts`

The remaining columns are treated as candidate environmental variables.

The function then:

1. Validates candidate column names using `check_illegal_strings()`.
2. Removes missing values from each candidate column.
3. Removes `NaN` values.
4. Removes infinite values (`Inf` and `-Inf`).
5. Retains only columns containing at least one remaining value.

Columns containing only missing, `NaN`, or infinite values are excluded.

# Returns

- `Vector{String}`: The names of environmental variable columns containing at
  least one finite, non-missing value.

# Throws

- `ErrorException`: If one or more candidate environmental variable names fail
  validation by `check_illegal_strings()`.
- `ErrorException`: If no valid environmental variables remain after filtering.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials |> simulate_environments;

julia> df = load_environments_df("simulated_environments.tsv");

julia> sort(extract_environment_variables(df)) == sort(names(df)[3:end])
true
```
"""
function extract_environment_variables(df::DataFrame; verbose::Bool = false)::Vector{String}
    id_cols = [
        "measurements",
        "sites",
        "replications",
        "blocks",
        "rows",
        "cols",
        "experiments",
        "treatments",
        "dates",
        "",
        "layouts",
    ]
    env_names = setdiff(names(df), id_cols)
    try
        check_illegal_strings(String.(env_names))
    catch e
        new_error = join(["Illegal string/s in the list of \"$env_names\"!\n", sprint(showerror, e)])
        error(new_error)
    end
    for env in env_names
        # env = env_names[1]
        y =
            df[!, env] |>
            x ->
                filter(xi -> !ismissing(xi), x) |>
                x -> filter(xi -> try
                    !isnan(xi);
                catch
                    ;
                    false;
                end, x) |> x -> filter(xi -> try
                    !isinf(xi);
                catch
                    ;
                    false;
                end, x)
        if length(y) < 1
            filter!(x -> x != env, env_names)
        end
    end
    if length(env_names) < 1
        env_names = setdiff(names(df), id_cols)
        error("Found $(length(env_names)) candidate envs but were all non-numeric: [\"$(join(env_names, "\", \""))\"].")
    end
    if verbose
        println("Found $(length(env_names)) envs: [\"$(join(env_names, "\", \""))\"].")
    end
    String.(env_names)
end
