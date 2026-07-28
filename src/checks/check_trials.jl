"""
    check_trials(
        df::DataFrame,
    )::Nothing

Validate that a DataFrame conforms to the expected structure and content of a
trial dataset.

The function verifies that all required trial columns are present, validates the
contents of string-valued columns against the project's naming conventions, and
checks that only expected fields are stored using numeric data types.

Required columns are determined from the `Trials` structure definition, excluding
phenotype and trait matrices. String columns are validated using
`check_illegal_strings`, whilst numeric columns are checked against a predefined
list of permitted numeric fields.

# Arguments

- `df::DataFrame`: Trial dataset to validate.

# Returns

- `Nothing`: Returned when all validation checks pass successfully.

# Throws

- `ErrorException`: If one or more required columns are missing.
- `ErrorException`: If a string column contains illegal characters or strings.
- `ErrorException`: If unexpected numeric columns are detected.

# Notes

- Required columns are derived from the fields of the `Trials` structure.
- Fields containing `phenotypes` or `traits` are excluded from the required-column
  check.
- String-valued columns are validated using `check_illegal_strings`.
- Numeric columns are expected only for:
  `years`, `measurements`, `replications`, `blocks`, `rows`, and `cols`.
- Any additional numeric columns are treated as potential data-formatting errors
  and will cause validation to fail.
- The function performs validation only and does not modify the input
  `DataFrame`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> isnothing(check_trials(df))
true
```
"""
function check_trials(df::DataFrame)::Nothing
    required_columns = sort(
        filter(
            x -> isnothing(match(Regex("phenotypes|traits"), x)),
            String.(string.(collect(fieldnames(Trials)))),
        ),
    )
    missing_columns = filter(x -> x∉names(df), required_columns)
    if length(missing_columns) > 0
        error("Missing columns: [\"$(join(missing_columns, "\", \""))\"].")
    end
    numeric_columns = String[]
    for x in required_columns
        # x = required_columns[end]
        if eltype(df[!, x]) <: AbstractString
            try
                check_illegal_strings(String.(unique(df[!, x])))
            catch e
                new_error =
                    join(["Illegal string in the \"$x\" column!\n", sprint(showerror, e)])
                error(new_error)
            end
        else
            push!(numeric_columns, x)
        end
    end
    unexpected_numeric_columns = filter(
        x -> x∉["years", "measurements", "replications", "blocks", "rows", "cols"],
        numeric_columns,
    )
    if length(unexpected_numeric_columns) > 0
        error(
            "Unexpected numeric column/s: [\"$(join(unexpected_numeric_columns, "\", \""))\"]",
        )
    end
    nothing
end
