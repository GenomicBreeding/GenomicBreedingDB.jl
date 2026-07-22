"""
    delete_names!(
        conn::LibPQ.Connection;
        df::DataFrame,
        table::String,
        df_col::String,
        verbose::Bool=false,
    )::Nothing

Delete records from a database table by matching names contained in a DataFrame
column.

The function extracts unique names from the specified DataFrame column and removes
matching records from the target database table. Deletion criteria are
constructed using `Filter` objects and translated into parameterised SQL
statements via `concat_filters`, ensuring consistent filtering behaviour across
database operations.

All delete operations are performed within a single transaction. If an error
occurs during processing, the transaction is rolled back and the original
exception is rethrown.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing names to be removed.
- `table::String`: Name of the target database table.
- `df_col::String`: Name of the DataFrame column containing names to delete.
- `verbose::Bool=false`: If `true`, display progress information and a summary of
  deleted records.

# Returns

- `Nothing`: Matching records are removed from the database.

# Throws

- `ErrorException`: If `df_col` does not exist in the DataFrame.
- Any database exception raised during deletion is rethrown after the transaction
  is rolled back.

# Notes

- Names are converted to strings, sorted, and deduplicated before processing.
- Delete statements are generated using `Filter` and `concat_filters`.
- SQL parameters are supplied separately from the query text to support safe,
  parameterised execution.
- Delete operations are wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Progress reporting is available when `verbose=true`.
- The function attempts a delete operation for each supplied name regardless of
  whether a matching record exists.
- The function permanently removes matching records from the specified table.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> df = simulate_genomes() |> simulate_trials |> tabularise;

julia> df.entries = string.("test_delete_names-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> df_before = execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> delete_names!(conn, df=df, table="entries", df_col="entries");

julia> df_after = execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> nrow(df_before) > nrow(df_after)
true

julia> close(conn);
```
"""
function delete_names!(
    conn::LibPQ.Connection;
    df::DataFrame,
    table::String,
    df_col::String,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = simulate_genomes() |> simulate_trials |> tabularise
    # df.entries = string.("test_delete_names-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);
    # insert_names!(conn, df=df, table="entries", df_col="entries")
    # extract_table(conn, "entries")
    # table = "entries"
    # df_col = "entries"
    # verbose::Bool = true
    if df_col∉names(df)
        throw(
            "The \"$df_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    uploaded_names = select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
    counter = 0
    pb = ProgressMeter.Progress(length(uploaded_names), "Deleting names listed in \"$df_col\" from \"$table\" table...")
    execute(conn, "BEGIN")
    try
        for x in uploaded_names
            # x = uploaded_names[1]
            sql, par = concat_filters([Filter(conn, table=table, field="name", filter_in=[x])])
            execute(
                conn,
                join(vcat(["DELETE FROM $table WHERE 1 = 1"], sql), " "),
                par,
            )
            counter += 1
            verbose ? ProgressMeter.next!(pb) : nothing
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Removed $counter names in the \"$table\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end
