
"""
    delete_names!(
        conn::LibPQ.Connection;
        df::DataFrame,
        table::String,
        df_col::String,
        verbose::Bool=false,
    )::Nothing

Delete named records from a database table.

This function extracts unique values from a specified DataFrame column and
removes matching records from a database table using the table's `name`
column.

Only names that already exist in the database table are deleted. Names that
do not exist are silently ignored.

# Arguments

- `conn::LibPQ.Connection`: An open PostgreSQL connection.
- `df::DataFrame`: A DataFrame containing names to delete.
- `table::String`: Name of the database table from which records should be
  removed.
- `df_col::String`: Name of the DataFrame column containing the values to
  delete.

# Keyword Arguments

- `verbose::Bool=false`: If `true`, displays a progress bar and reports the
  number of deleted records.

# Details

The function:

1. Verifies that `df_col` exists in `df`.
2. Extracts the unique values from `df[df_col]`.
3. Retrieves all existing names from the specified database table.
4. Deletes records whose `name` field matches a value extracted from the
   DataFrame.
5. Executes all deletions within a single database transaction.

Names present in `df` but absent from the database are ignored.

# Transaction Behaviour

All deletions are executed within a single database transaction.

- On success, the transaction is committed.
- On failure, the transaction is rolled back and the original exception is
  rethrown.

# Returns

`Nothing`.

# Throws

- `ErrorException`: If `df_col` does not exist in `df`.
- `ErrorException`: If the specified database table does not exist or cannot
  be queried.
- Any exception generated during query execution or transaction handling.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial();

julia> conn = dbconnect();

julia> df = load_trial_df(fname);

julia> df.entries = string.("test_delete_names-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> df_before = execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> delete_names!(conn, df=df, table="entries", df_col="entries");

julia> df_after = execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> nrow(df_before) > nrow(df_after)
true

julia> close(conn); rm(fname);
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
    # df = CSV.read(simulate(), DataFrame)
    # table = "entries"
    # df_col = "entries"
    # verbose::Bool = true
    if df_col∉names(df)
        throw(
            "The \"$df_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    uploaded_names = select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
    existing_names = let
        df_tmp = try
            DataFrame(execute(conn, "SELECT name FROM $table;"))
        catch
            throw(
                "Missing \"$table\" table in the database! (Note that the existence of the 'name' field is checked every time a connection to the database is made via `dbconnect()`.)",
            )
        end
        String.(string.(df_tmp[:, 1]))
    end
    counter = 0
    pb = ProgressMeter.Progress(length(uploaded_names), "Deleting names listed in \"$df_col\" from \"$table\" table...")
    execute(conn, "BEGIN")
    try
        for x in uploaded_names
            # x = uploaded_names[1]
            if x ∈ existing_names
                execute(
                    conn,
                    """
                    DELETE FROM $table
                    WHERE name = \$1;
                    """,
                    [x],
                )
                counter += 1
                verbose ? ProgressMeter.next!(pb) : nothing
            end
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
