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
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> traits = extract_traits(df);

julia> traits == filter(x -> !isnothing(match(Regex("^trait_"), x)), names(df))
true
```
"""
function extract_traits(df::DataFrame; verbose::Bool = false)::Vector{String}
    trial_columns = sort(
        filter(
            x -> isnothing(match(Regex("phenotypes|traits"), x)),
            String.(string.(collect(fieldnames(Trials)))),
        ),
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
        new_error = join([
            "Illegal string/s in the list of \"$trait_names\"!\n",
            sprint(showerror, e),
        ])
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
                x ->
                    filter(xi -> try
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
        println(
            "Found $(length(trait_names)) traits: [\"$(join(trait_names, "\", \""))\"].",
        )
    end
    String.(trait_names)
end
