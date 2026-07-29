"""
    check(
        df::DataFrame,
        col::String,
    )::Nothing

Validate that a DataFrame contains a specified column and, when applicable,
validate the string values within that column.

The function verifies that `col` exists in the supplied `DataFrame`. If the column
contains string values, all unique entries are validated using
`check_illegal_strings` to ensure they conform to the project's naming and
identifier conventions.

If validation fails, the underlying error is rethrown with additional context
identifying the offending column.

# Arguments

- `df::DataFrame`: DataFrame to validate.
- `col::String`: Name of the column that must exist and, if applicable, contain
  valid string values.

# Returns

- `Nothing`: Returned when the column exists and all validation checks pass.

# Throws

- `ErrorException`: If the specified column does not exist in the DataFrame.
- `ErrorException`: If the column contains invalid string values.
- Any exception raised by `check_illegal_strings`, wrapped with contextual
  information identifying the column being validated.

# Notes

- String validation is performed only when the column element type is a subtype
  of `AbstractString`.
- Unique string values are validated to avoid redundant checks.
- Validation of string content is delegated to `check_illegal_strings`.
- The function does not modify the input `DataFrame`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df = DataFrame(aye=["abc", "def", "ghi"], nay=["hello world ^_^", "wildcard*", "slash/slash"]);

julia> try isnothing(check(df, "aye")); catch; false; end
true

julia> try isnothing(check(df, "nay")); catch; false; end
false
```
"""
function check(df::DataFrame, col::String)
    if col∉names(df)
        error(
            "The \"$col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    if eltype(df[!, col]) <: AbstractString
        try
            check_illegal_strings(String.(unique(df[!, col])))
        catch e
            new_error = join(["Illegal string in the \"$col\" column!\n", sprint(showerror, e)])
            error(new_error)
        end
    end
    nothing
end

"""
    check(
        df::DataFrame,
    )::Nothing

Validate that a DataFrame conforms to the expected structure of a phenotype or
environmental data table.

The function checks that the DataFrame contains the required fields needed for
long-format observational data. Field names ending in `_id` are normalised by
removing the suffix before validation, allowing both identifier-based and
name-based representations of the data.

The dataset must contain all core experimental design fields together with a
`value` field and exactly one of either `trait` or
`environment_variable`.

# Arguments

- `df::DataFrame`: Data table to validate.

# Returns

- `Nothing`: Returned when the DataFrame satisfies the expected structure.

# Throws

- `ErrorException`: If required fields are missing from the DataFrame.

# Notes

- Identifier fields ending in `_id` are treated as equivalent to their
  corresponding name-based fields.
- Required fields include:
  `experiment`, `site`, `treatment`, `layout`, `measurement`, `entry`,
  and `value`.
- Exactly one of `trait` or `environment_variable` may be omitted.
- The function supports both phenotype and environmental data tables.
- Validation is limited to field presence and does not verify data types or
  field contents.
- The function performs validation only and does not modify the input
  `DataFrame`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df_okay_phe = DataFrame(experiment=[], site=[], treatment=[], layout=[], measurement=[], entry=[], trait=[], value=[]);

julia> df_okay_env = DataFrame(experiment=[], site=[], treatment=[], layout=[], measurement=[], entry=[], environment_variable=[], value=[]);

julia> df_nope = DataFrame(experiment=[], site=[]);

julia> try isnothing(check(df_okay_phe)); catch; false; end
true

julia> try isnothing(check(df_okay_env)); catch; false; end
true

julia> try isnothing(check(df_nope)); catch; false; end
false
```
"""
function check(df::DataFrame)::Nothing
    # conn = dbconnect()
    # df = extract_table(conn, "phenotype_data")
    expected_fields = [
        "experiment",
        "site",
        "treatment",
        "layout",
        "measurement",
        "entry",
        "trait",
        "environment_variable",
        "value",
    ]
    fields = replace.(names(df), Regex("_id\$") => "")
    missing_fields = filter(x -> x ∉ fields, expected_fields)
    okay = (
        (length(missing_fields) == 0) ||
        ("trait" ∈ missing_fields) && (sort(missing_fields) == ["entry", "trait"]) ||
        (missing_fields == ["environment_variable"])
    )
    if !okay
        error("Missing field/s:\n\t- $(join(missing_fields, "\n\t- "))")
    end
    nothing
end

"""
    check(
        df::DataFrame,
        field::String,
        expected_names::Vector{String};
        proposed_name::Union{Nothing,String}=nothing,
    )::Nothing

Validate that observed values for a field belong to a predefined set of allowed
names.

The function checks that all observed values associated with a field are present
within a supplied list of valid values. Values may be obtained either from a
column in a `DataFrame` or from a single value supplied via `proposed_name`.

If the specified field is present in the DataFrame, all unique values from that
column are validated. Otherwise, if `proposed_name` is provided, that value is
validated directly. If neither source is available, an informative error is
raised describing how the missing information should be supplied.

Any values not found in `expected_names` are collected and reported together in a
single error message.

# Arguments

- `df::DataFrame`: DataFrame containing values to validate.
- `field::String`: Name of the field to validate.
- `expected_names::Vector{String}`: Collection of permitted values.
- `proposed_name::Union{Nothing,String}=nothing`: Optional value to validate when
  the specified field is not present in the DataFrame.

# Returns

- `Nothing`: Returned when all observed values match the allowed values.

# Throws

- `ErrorException`: If neither `field` exists in the DataFrame nor
  `proposed_name` is supplied.
- `ErrorException`: If one or more observed values are not present in
  `expected_names`.

# Notes

- Validation is performed against unique observed values only.
- DataFrame values take precedence over `proposed_name` when both are supplied.
- If the field is not present in the DataFrame, the function expects an
  appropriate singular parameter value (derived from the field name) to be
  supplied via `proposed_name`.
- All invalid values are reported simultaneously to simplify data correction.
- The error message includes the complete list of valid values.
- This function is useful for validating controlled vocabularies, categorical
  variables, ontology terms, configuration values, and user-supplied metadata.
- The function performs validation only and does not modify the input DataFrame.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> try isnothing(check(df, "entry_types", ["cultivar", "population", "individual", "family"], proposed_name="family")); catch; false; end
true

julia> try isnothing(check(df, "entry_types", ["cultivar", "population", "individual", "family"], proposed_name="clone")); catch; false; end
false
```
"""
function check(
    df::DataFrame,
    field::String,
    expected_names::Vector{String};
    proposed_name::Union{Nothing,String} = nothing,
)::Nothing
    # simulate_genomes() |> simulate_trials
    # df = load_trial_df("simulated_trials.tsv", missing_strings=["missing", "NA", "na", "N/A", "n/a", ""])
    # field::String = "entry_types"
    # expected_names = ["cultivar", "population", "individual", "family"]
    # proposed_name::Union{Nothing,String} = "family"
    observed_names = if field ∈ names(df)
        unique(df[!, field])
    elseif !isnothing(proposed_name)
        [proposed_name]
    else
        error(
            string(
                "Please define the `$field` as either a column in the DataFrame (or input file) or ",
                "supply it as `",
                replace(field, Regex("s\$")=>""),
                "` parameter!",
            ),
        )
    end
    errors = String[]
    for name in observed_names
        # name = observed_names[1]
        if name∉expected_names
            push!(errors, name)
        end
    end
    if length(errors) > 0
        error(
            string(
                "Invalid value/s in `df.$field` or `proposed_name`:\n\t- \"",
                join(errors, "\"\n\t- \""),
                "\"\n",
                "Choose from: [\"",
                join(expected_names, "\", \""),
                "\"].",
            ),
        )
    end
    nothing
end
