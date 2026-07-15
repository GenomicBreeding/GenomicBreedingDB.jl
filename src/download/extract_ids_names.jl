
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
- `ErrorException`: If the specified table and/or `name` field does not exist in the database.
- `BoundsError`: If a supplied name or pattern does not match any record in the table.

# Notes
- When `is_like=false`, zero or one exact match for each item in `names` is expected.
- When `is_like=true`, zero or more matches for each case-insensitive pattern in `names` is expected.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

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
        df = DataFrame(id=String[], name=String[])
        for name in names
            # name = names[1]
            df_tmp = DataFrame(execute(conn, "SELECT id,name FROM $table WHERE name ILIKE \$1", ["%$(replace(name, "_" => "\\_"))%"]))
            df = vcat(df, df_tmp)
        end
        df
    end
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end




# TODO
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
