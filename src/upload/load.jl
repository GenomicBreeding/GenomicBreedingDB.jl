"""
    load_trial_df(
        fname::String;
        missing_strings::Vector{String}=String[],
    )::DataFrame

Load trial data from a delimited file and return it as a `DataFrame`.

The function first verifies that the specified file exists. If no custom missing
value strings are provided, it attempts to parse the file using
`GenomicBreedingIO.readdelimited` and convert the result into a tabular
representation. If this process fails, the file is read directly using `CSV.read`
with a predefined set of missing value markers.

When custom missing value strings are supplied, the file is read directly using
`CSV.read` and the specified missing value definitions. If present, a column named
`#years` is automatically renamed to `years`.

# Arguments

- `fname::String`: Path to the trial data file.
- `missing_strings::Vector{String}=String[]`: Strings that should be interpreted as
  missing values when reading the file.

# Returns

- `DataFrame`: Trial data loaded from the specified file.

# Throws

- `ErrorException`: If the specified file does not exist.

# Notes

- When `missing_strings` is empty, the function preferentially uses
  `GenomicBreedingIO.readdelimited` followed by
  `GenomicBreedingCore.tabularise`.
- If parsing with `GenomicBreedingIO.readdelimited` fails, the file is read using
  `CSV.read` as a fallback.
- The default missing value markers are `"missing"`, `"NA"`, `"na"`, `"N/A"`,
  `"n/a"`, and `""`.
- A column named `#years` is renamed to `years` when present to improve
  compatibility with downstream processing.

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
    extract_traits(
        df::DataFrame;
        verbose::Bool=false,
    )::Vector{String}

Identify trait columns in a trial `DataFrame` and return their names.

The function determines candidate trait columns by excluding known metadata fields and
standard trial attributes. Candidate names are validated to ensure they do not contain
illegal strings and are then filtered to retain only columns containing at least one
valid numeric value.

Columns named `id` or ending with `_id` are excluded from consideration. Columns
containing only missing, `NaN`, or infinite values are also removed. If no valid trait
columns remain after filtering, an error is raised describing the rejected
candidates.

# Arguments

- `df::DataFrame`: Trial data containing metadata columns and one or more trait
  columns.
- `verbose::Bool=false`: If `true`, print the detected trait names to standard
  output.

# Returns

- `Vector{String}`: Names of columns identified as valid trait variables.

# Throws

- `ErrorException`: If candidate trait names contain illegal strings.
- `ErrorException`: If no valid numeric trait columns are found.

# Notes

- Standard trial-related columns defined by `Trials` are automatically excluded.
- Additional metadata columns such as `dates`, `species`, `experiments`,
  `treatments`, and related descriptors are excluded from trait detection.
- Columns named `id` or matching the pattern `*_id` are ignored.
- A trait column must contain at least one non-missing, finite numeric value to be
  considered valid.
- Returned trait names are sorted according to the order they appear in the input
  `DataFrame`.

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
            "missing", "NA", "na", "N/A", "n/a", ""
        ],
    )::DataFrame

Load environmental data from a delimited file and return it as a `DataFrame`.

The function verifies that the specified file exists before reading it using
`CSV.read`. Values matching any of the supplied missing-value strings are converted
to `missing` during import.

# Arguments

- `fname`: Path to the environmental data file.
- `missing_strings::Vector{String}=["missing", "NA", "na", "N/A", "n/a", ""]`:
  Strings that should be interpreted as missing values when reading the file.

# Returns

- `DataFrame`: Environmental data loaded from the specified file.

# Throws

- `ErrorException`: If the specified file does not exist.

# Notes

- Data are imported using `CSV.read`.
- The default missing value markers are `"missing"`, `"NA"`, `"na"`, `"N/A"`,
  `"n/a"`, and `""`.
- Any value matching an entry in `missing_strings` is converted to `missing` in the
  resulting `DataFrame`.

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

Identify environmental variable columns in a `DataFrame` and return their names.

The function determines candidate environmental variables by excluding known
identifier and metadata columns. Candidate names are validated to ensure they do not
contain illegal strings and are then filtered to retain only columns containing at
least one valid numeric value.

Columns containing only missing, `NaN`, or infinite values are excluded from the
result. If no valid environmental variables remain after filtering, an error is
raised describing the rejected candidates.

# Arguments

- `df::DataFrame`: Environmental data containing identifier columns and one or more
  environmental variables.
- `verbose::Bool=false`: If `true`, print the detected environmental variable names
  to standard output.

# Returns

- `Vector{String}`: Names of columns identified as valid environmental variables.

# Throws

- `ErrorException`: If candidate variable names contain illegal strings.
- `ErrorException`: If no valid numeric environmental variables are found.

# Notes

- Known identifier and metadata columns such as `measurements`, `sites`,
  `replications`, `blocks`, `rows`, `cols`, `experiments`, `treatments`, `dates`,
  and `layouts` are automatically excluded.
- Environmental variables must contain at least one non-missing, finite numeric
  value to be considered valid.
- Columns consisting entirely of missing, `NaN`, or infinite values are excluded.
- Returned names preserve the order in which columns appear in the input
  `DataFrame`.

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
