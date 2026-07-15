
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
  If `true`, perform pattern matching using SQL `LIKE`. In this case,
  entries in `names` contain SQL wildcard characters, i.e. "%\$(name)%".

# Returns
- `DataFrame`: A DataFrame with two columns:
  - `id`: The database IDs corresponding to the matched names.
  - `name`: The original input names or search patterns.

# Throws
- `ErrorException`: If the specified table and/or `name` field does not exist in the database.
- `BoundsError`: If a supplied name or pattern does not match any record in the table.

# Notes
- When `is_like=false`, each element of `names` is expected to match exactly one record.
- When `is_like=true`, the first matching `id` returned by the database is used for each pattern.

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
    # conn = dbconnect(); names = String["entry_01", "entry_04"]; table = "entries"; is_like = true
    df = DataFrame(id=String[], name=String[])
    for name in names
        # name = names[1]
        res = try
            if !is_like
                execute(conn, "SELECT id,name FROM $table WHERE name = \$1", [name])
            else
                execute(conn, "SELECT id,name FROM $table WHERE name LIKE \$1", ["%$(name)%"])
            end
        catch
            error("The table \"$table\" and/or \"name\" field does not exist.")
        end
        df = vcat(df, DataFrame(res))
    end
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end




# TODO
function extract_names(conn::LibPQ.Connection; ids::Vector{String}, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"; ids = extract_ids(conn, names=String["entry_001", "entry_100"], table=table).id;
    df = DataFrame(id=String[], name=String[])
    for id in ids
        # id = ids[1]
        res = try
            execute(conn, "SELECT id,name FROM $table WHERE id = \$1", [id])
        catch
            error("The table \"$table\" and/or \"name\" field does not exist.")
        end
        df = vcat(df, DataFrame(res))
    end
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end
