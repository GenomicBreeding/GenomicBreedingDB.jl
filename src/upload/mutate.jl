"""
    add_col!(df::DataFrame; col::String, value::Union{Nothing, String})::Nothing

Verifies and adds a column in a DataFrame filling it with a single specified string.

# Arguments
- `df::DataFrame`: The DataFrame to modify.
- `col::String`: The name of the column to add or verify.
- `value::Union{Nothing, String}`: The value to assign to the column. If `nothing`, the column must already exist in the DataFrame.

# Behaviour
- If the column already exists in `df`, the function warns the user and ignores the `value` argument.
- If the column does not exist and `value` is `nothing`, an error is thrown.
- If the column does not exist and `value` is a string, the column is created and all rows are assigned the `value`.
- Both the column name and value are validated using `check_illegal_strings()` to ensure they contain only allowed characters for database identifiers and names.

# Throws
- `String`: If the specified column is not found in the DataFrame and no value is provided.
- `String`: If the column name or value contains illegal characters or non-ASCII content (see `check_illegal_strings()` for details).

# Returns
- `nothing`

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> size(df)
(12800, 14)

julia> add_col!(df, col="some_new_column", value="Dolorem ipsum")

julia> size(df)
(12800, 15)
```
"""
function add_col!(df::DataFrame; col::String, value::Union{Nothing,String})::Nothing
    # df = CSV.read(simulate_trial(), DataFrame); col = "species"; value = nothing
    # df = CSV.read(simulate_trial(), DataFrame); col = "species"; value = "Lolium multiflorum"
    if col ∈ names(df)
        if !isnothing(value)
            @warn("Using \"$col\" col in the dataframe instead of the supplied \"$col = $value\".")
        end
    else
        if isnothing(value)
            error("Please define the value to be inserted into the \"$col\" column of the dataframe.")
        end
        try
            check_illegal_strings([col, value])
        catch e
            new_error =
                join(["Illegal string/s in new column name [$col] and/or its value [$value]!\n", sprint(showerror, e)])
            error(new_error)
        end
        df[!, col] .= value
    end
    nothing
end


"""
    parse_layouts!(df::DataFrame; is_trial::Bool=true)::Nothing

Parse and standardise trial layout columns in a DataFrame.

This function processes the layout-related columns `:replications`, `:blocks`,
`:rows`, and `:cols`, converting them to `Int64` vectors when necessary and
reconstructing the `:layouts` column from the parsed values.

# Arguments
- `df::DataFrame`: A DataFrame containing layout information.

# Keyword Arguments
- `is_trial::Bool=true`: If `true`, validates `df` using `validate_trials()`
  before parsing. Set to `false` to skip validation.

# Details
For each of the layout columns `:replications`, `:blocks`, `:rows`, and `:cols`:

1. If the column is already a `Vector{Int64}`, it is left unchanged.
2. Otherwise, each value is converted to an integer by repeatedly splitting on
   `"_"`, `"-"`, and `"|"` and taking the final element after each split.
3. The resulting strings are parsed as `Int64`.

After successful parsing, the `:layouts` column is regenerated using the format

`"<replication>-<block>-<row>-<col>"`.

Examples of supported input values include:

- `"rep_1"` → `1`
- `"block-2"` → `2`
- `"column|10"` → `10`

# Returns
`Nothing`.

The input DataFrame is modified in-place.

# Throws
- `ErrorException`: If any of the layout columns cannot be parsed as integers.
  The error message identifies the offending column.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df_ids = DataFrame(entries=repeat([""],3), measurements=repeat([""],3), populations=repeat([""],3), seasons=repeat([""],3), sites=repeat([""],3), years=repeat([""],3));

julia> replications=[1, 2, 3]; blocks=[1, 1, 2]; rows=[1, 2, 3]; cols=[10, 11, 12];

julia> df_expected = hcat(df_ids, DataFrame(replications=replications, blocks=blocks, rows=rows, cols=cols, layouts=string.(replications, "-", blocks, "-", rows, "-", cols)));

julia> df_1 = copy(df_expected);

julia> parse_layouts!(df_1)

julia> df_1 == df_expected
true

julia> df_input = hcat(df_ids, DataFrame(replications=["rep_1", "rep_2", "3"], blocks=["b_1", "1", "block-2"], rows=[1, 2, 3], cols=["column|10", "COL-11", "column_12"]));

julia> df_2 = copy(df_input);

julia> parse_layouts!(df_2)

julia> df_2 == df_expected
true
```
"""
function parse_layouts!(df::DataFrame; is_trial::Bool = true)::Nothing
    is_trial ? validate_trials(df) : nothing
    for f in [:replications, :blocks, :rows, :cols]
        # f = :replications
        isa(df[!, f], Vector{Int64}) ? continue : nothing
        df[!, f] = try
            df[!, f] |>
            x ->
                [split(xi, "_")[end] for xi in x] |>
                x ->
                    [split(xi, "-")[end] for xi in x] |> x -> [split(xi, "|")[end] for xi in x] |> x -> [parse(Int64, xi) for xi in x]
        catch
            error("Cannot parse $(f)!")
        end
    end
    df.layouts = string.(df.replications, "-", df.blocks, "-", df.rows, "-", df.cols)
    nothing
end

"""
    add_measurement_dates!(df::DataFrame; measurement_dates::Union{Nothing, Dict{String, String}})::Nothing

Add or validate measurement dates to a DataFrame.

This function either uses an existing "dates" column in the DataFrame or maps measurement identifiers
to their corresponding dates from a dictionary. It validates date formats and ensures all measurements
have associated dates.

# Arguments
- `df::DataFrame`: The input DataFrame containing a "measurements" column (must be a `Vector{String}` or will be converted).
- `measurement_dates::Union{Nothing, Dict{String, String}}`: Optional dictionary mapping measurement
  identifiers (keys) to date strings (values). If `nothing`, the function expects a "dates" column
  in the DataFrame.

# Details
- **Date Format**: Dates must be in "yyyy-mm-dd" format with integer values.
- **Measurements Column**: The "measurements" column is converted to `Vector{String}` if needed.
- **Column Handling**: If a "dates" column exists in `df` and `measurement_dates` is provided,
  a warning is issued and the DataFrame column takes precedence.
- **Validation**: All measurements in the DataFrame must have corresponding dates defined,
  either in the "dates" column or in the `measurement_dates` dictionary.

# Errors
Throws an error if:
- The "dates" column contains invalid date formats.
- `measurement_dates` is `nothing` and no "dates" column exists in the DataFrame.
- A measurement in the DataFrame is not found in the `measurement_dates` dictionary.
- A date string in `measurement_dates` has an invalid format.

# Returns
`nothing`

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname); measurements = String.(unique(df.measurements));

julia> measurement_dates::Dict{String,String} = Dict(); [measurement_dates[x] = x for x in measurements];

julia> df_1 = deepcopy(df); df_2 = deepcopy(df);

julia> add_measurement_dates!(df_1);

julia> add_measurement_dates!(df_2, measurement_dates=measurement_dates);

julia> isa(df_1.dates, Vector{DateTime})
true

julia> df_1.dates == df_2.dates
true
```
"""
function add_measurement_dates!(df::DataFrame; measurement_dates::Union{Nothing,Dict{String,String}} = nothing)::Nothing
    # df = CSV.read(simulate_trial(), DataFrame); measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # df = CSV.read(simulate_trial(), DataFrame); df[!, "dates"] = String.(df.measurements); measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # df = CSV.read(simulate_trial(), DataFrame); measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in unique(df.measurements)]
    if "dates" ∈ names(df)
        if !isnothing(measurement_dates)
            @warn("Using the \"dates\" col in the dataframe.")
        end
        dates = unique(df.dates) # dates[1] = "2025/JA/01"
        if !isa(dates, Vector{DateTime}) && (sum(.!validate_date.(dates)) > 0)
            error(
                "Invalid date format/s: [\"$(join(dates, "\", \""))\"]. We expect \"yyyy-mm-dd\" format, where all values are integers.",
            )
        end
    else
        if "measurements"∉names(df)
            error(
                "SInce there is no \"dates\" column in the dataframe we expect a \"measurements\" column instead which we can parse into dates of use the \"measurement_dates\" parameter to map into their respective dates.",
            )
        else
            if !(eltype(df[!, :measurements]) <: AbstractString)
                df.measurements = [String("$x") for x in df.measurements]
            end
            if isnothing(measurement_dates)
                # Parse df.measurements into dates
                measurements = sort(String.(unique(df.measurements)))
                df[!, "dates"] .= Dates.now()
                for m in measurements
                    # m = measurements[1]
                    validate_date(m)
                    idx = findall(df.measurements .== m)
                    length(idx) == 0 ? error("Measurement \"$m\" not found in the dataframe!") : nothing
                    df.dates[idx] .= Date(m, dateformat"yyyy-mm-dd")
                end
            else
                # Use the measurement_dates parameter to map the measurements with their respective dates
                measurements = sort(String.(unique(df.measurements)))
                measurements_input = sort(String.(keys(measurement_dates)))
                if measurements != sort(measurements ∩ measurements_input)
                    error(
                        "Please define all the dates for all the measurements. We have the following measurements: [$(join(measurements, ", "))] but only the following were defined in the input: [$(join(measurements_input, ", "))]",
                    )
                end
                df[!, "dates"] .= Dates.now()
                for (k, v) in measurement_dates
                    # k = string.(keys(measurement_dates))[1]; v = measurement_dates[k]
                    # v = "10062026"
                    # v = "2025/03/dd"
                    validate_date(v)
                    idx = findall(df.measurements .== k)
                    # println("k=$k; v=$v; length(idx)=$(length(idx))")
                    length(idx) == 0 ? error("Measurement \"$k\" not found in the dataframe!") : nothing
                    df.dates[idx] .= Date(v, dateformat"yyyy-mm-dd")
                end
            end
        end
    end
    nothing
end
