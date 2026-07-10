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
    uploaded_names =
        select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
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
    pb = ProgressMeter.Progress(
        length(uploaded_names),
        "Deleting names listed in \"$df_col\" from \"$table\" table...",
    )
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
