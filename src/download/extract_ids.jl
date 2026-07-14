"""
    extract_ids(conn::LibPQ.Connection; names::Vector{String}, table::String)::DataFrame

Extract database IDs for a given list of names from a specified table.

# Arguments
- `conn::LibPQ.Connection`: A connection to a PostgreSQL database.
- `names::Vector{String}`: Vector of names to look up in the table.
- `table::String`: The name of the table to query.

# Returns
- `DataFrame`: A DataFrame with two columns: `id` (the database IDs) and `name` (the original names).

# Throws
- `String`: An error message if the table and/or "name" field does not exist in the database.

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
function extract_ids(conn::LibPQ.Connection; names::Vector{String}, table::String)::DataFrame
    ids = String[]
    for name in names
        # name = names[1]
        res = try
            execute(conn, "SELECT id FROM $table WHERE name = \$1", [name])
        catch
            error("The table \"$table\" and/or \"name\" field does not exist.")
        end
        push!(ids, DataFrame(res).id[1])
    end
    DataFrame(id = ids, name = names)
end
