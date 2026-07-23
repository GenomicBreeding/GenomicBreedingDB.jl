"""
    extract_ids(
        conn::LibPQ.Connection;
        names::Vector{String},
        table::String,
        is_like::Bool=false,
    )::DataFrame

Retrieve identifier and name pairs from a database table for a collection of names.

When `is_like` is `false`, names are matched exactly using a SQL `ANY` condition.
When `is_like` is `true`, each provided name is matched using a case-insensitive
`ILIKE` search, allowing partial matches. Matching records are returned as a
`DataFrame` containing `id` and `name` columns.

The function validates that the specified table exists and contains a `name` column
before executing any queries. Missing identifiers are removed from the result and
all returned values are converted to `String`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `names::Vector{String}`: Names to search for in the specified table.
- `table::String`: Name of the table containing `id` and `name` fields.
- `is_like::Bool=false`: If `true`, perform case-insensitive partial matching using
  `ILIKE`; otherwise perform exact matching.

# Returns

- `DataFrame`: Table containing matching `id` and `name` pairs.

# Notes

- The specified table must contain both `id` and `name` columns.
- Partial matching is performed individually for each element in `names`.
- Underscore characters in search strings are escaped when using `ILIKE` to prevent
  unintended wildcard matching.
- Rows with missing identifiers are removed from the returned result.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_extract_ids_$(Dates.time() |> x -> replace("$x", "." => "_"))-", df.entries);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> df_entries = extract_ids(conn, names=sort(unique(df.entries)), table="entries");

julia> sort(df_entries.name) == sort(unique(df.entries))
true

julia> close(conn);
```
"""
function extract_ids(conn::LibPQ.Connection; names::Vector{String}, table::String, is_like::Bool = false)::DataFrame
    # conn = dbconnect(); names = String["entry_001", "entry_004"]; table = "entries"; is_like = true
    check(conn, table)
    check(conn, table, "name")
    df = if !is_like
        DataFrame(execute(conn, "SELECT id,name FROM $table WHERE name = ANY(\$1)", [names]))
    else
        df = DataFrame(id = String[], name = String[])
        for name in names
            # name = names[1]
            df_tmp = DataFrame(
                execute(conn, "SELECT id,name FROM $table WHERE name ILIKE \$1", ["%$(replace(name, "_" => "\\_"))%"]),
            )
            df = vcat(df, df_tmp)
        end
        df
    end
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end

"""
    extract_names(
        conn::LibPQ.Connection;
        ids::Vector{String},
        table::String,
    )::DataFrame

Retrieve name values associated with a collection of identifiers from a database
table.

The function queries the specified table for records whose identifiers match the
provided `ids` and returns the corresponding `id` and `name` pairs as a
`DataFrame`. The table and required `name` column are validated before the query is
executed.

Rows with missing identifiers are removed from the result, and both `id` and `name`
columns are converted to `String` values before being returned.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `ids::Vector{String}`: Identifiers to look up in the specified table.
- `table::String`: Name of the table containing `id` and `name` fields.

# Returns

- `DataFrame`: Table containing matching `id` and `name` pairs.

# Notes

- The specified table must exist and contain a `name` column.
- Identifier matching is performed using a parameterised SQL `ANY` condition.
- Rows with missing identifiers are excluded from the returned result.
- Returned `id` and `name` values are converted to `String` for consistency.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_extract_names_$(Dates.time() |> x -> replace("$x", "." => "_"))-", df.entries);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> df_entries_0 = extract_ids(conn, names=sort(unique(df.entries)), table="entries");

julia> df_entries_1 = extract_names(conn, ids=df_entries_0.id, table="entries");

julia> close(conn);

julia> df_entries_0 == df_entries_1
true
```
"""
function extract_names(conn::LibPQ.Connection; ids::Vector{String}, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"; ids = extract_ids(conn, names=String["entry_001", "entry_100"], table=table).id;
    check(conn, table)
    check(conn, table, "name")
    df = DataFrame(execute(conn, "SELECT id,name FROM $table WHERE id = ANY(\$1)", [ids]))
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end
