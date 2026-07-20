
"""
    extract_ids(conn::LibPQ.Connection;
                names::Vector{String},
                table::String,
                is_like::Bool=false)::DataFrame

Extract database IDs for a given list of names from a specified table.

# Arguments
- `conn::LibPQ.Connection`: A connection to a PostgreSQL database.
- `names::Vector{String}`: Vector of names or search patterns to look up in the table.
- `table::String`: The name of the table to query.
- `is_like::Bool=false`: If `false`, perform exact matching using `=`.
  If `true`, perform case-insensitive pattern matching using SQL `ILIKE`. In this case,
  entries in `names` contain SQL wildcard characters, i.e. "%\$(name)%".
  Additionally underscores in `names` are escaped so that they are not translated as wildcards.

# Returns
- `DataFrame`: A DataFrame with two columns:
  - `id`: The database IDs corresponding to the matched names.
  - `name`: The original input names or search patterns.

# Throws
- An exception if `table` does not exist.
- An exception if the `name` field does not exist in `table`.
- An exception if `table` contains illegal characters.

# Notes
- When `is_like=false`, zero or one exact match for each item in `names` is expected.
- When `is_like=true`, zero or more matches for each case-insensitive pattern in `names` is expected.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_extract_ids_$(Dates.time() |> x -> replace("$x", "." => "_"))-", df.entries);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> df_entries = extract_ids(conn, names=sort(unique(df.entries)), table="entries");

julia> close(conn);

julia> df_entries.name == sort(unique(df.entries))
true
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
    extract_names(conn::LibPQ.Connection; ids::Vector{String}, table::String)::DataFrame

Extract names corresponding to a set of database identifiers.

The function queries the specified database table and returns the
`id` and `name` fields for all matching identifiers. Prior to querying,
the function validates that the table exists and contains a `name`
field using `check()`.

Any records with missing identifiers are removed from the result,
and both `id` and `name` columns are returned as `String` vectors.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `ids::Vector{String}`: Database identifiers to retrieve names for.
- `table::String`: Name of the database table containing `id` and
  `name` fields.

# Returns
- `DataFrame`: A DataFrame containing:
  - `id::String`
  - `name::String`

Only rows corresponding to identifiers present in `ids` are returned.

# Throws
- An exception if `table` does not exist.
- An exception if the `name` field does not exist in `table`.
- An exception if `table` contains illegal characters.

# Notes

The returned DataFrame may contain fewer rows than the number of
supplied identifiers if some identifiers are not present in the
database table.

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
