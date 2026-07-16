"""
    update_table_field_by_name!(conn::LibPQ.Connection; df::DataFrame, table::String, df_name_col::String, df_source_col::String, table_destination_field::String, verbose::Bool = false)::Nothing

Update a specific field in a database table by matching records based on a name column in a DataFrame.

# Arguments
- `conn::LibPQ.Connection`: Active database connection.
- `df::DataFrame`: Source DataFrame containing the data to update.
- `table::String`: Name of the target table in the database.
- `df_name_col::String`: Name of the column in `df` used to match records in the database table via the `name` field.
- `df_source_col::String`: Name of the column in `df` containing the values to update.
- `table_destination_field::String`: Name of the field in the database table to update. If the field name ends with "_id", the function will resolve foreign key references from the corresponding related table.
- `verbose::Bool`: If `true`, displays a progress bar during the update operation. Defaults to `false`.

# Returns
- `Nothing`

# Behaviour
- Validates that both source columns exist in the provided DataFrame.
- Validates that the target table and field exist in the database.
- Validates string columns using `check_illegal_strings()` to ensure they contain only allowed characters for database identifiers and names. The following characters are not allowed: `;`, `|`, `,`, `.`, `/`, `\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`, `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`, `?`. Non-ASCII characters are also rejected.
- Automatically handles foreign key relationships when the destination field ends with "_id" by looking up IDs from the related table.
- Updates the `updated_at` timestamp for each modified record (Note: the PostgreSQL schema does this automatically for the metadata table, hence redundant here but I like to be explicit here).
- Uses database transactions (BEGIN/COMMIT/ROLLBACK) to ensure atomicity.
- Throws an error if the number of affected rows is unexpected (i.e. ero or more than 1) or if the table is empty.
- Displays a progress meter if verbose mode is enabled.

# Throws
- `String`: If required columns don't exist in the DataFrame.
- `String`: If string columns contain illegal characters or non-ASCII content (see `check_illegal_strings()` for details).
- `String`: If the table or field doesn't exist in the database.
- `String`: If the table is empty before updating.
- `String`: If an unexpected number of rows are affected during update.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> df.entries = string.("test_update_table-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> df.species .= string.("test_update_table-", Dates.time() |> x -> replace(string(x), "." => "_"), "-Elysia chlorotica");

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> insert_names!(conn, df=df, table="species", df_col="species");

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="species", table_destination_field="species_id");

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> close(conn);

julia> sum(.!ismissing.(unique(df_before.species_id))) < sum(.!ismissing.(unique(df_after.species_id)))
true
```
"""
function update_table_field_by_name!(
    conn::LibPQ.Connection;
    df::DataFrame,
    table::String,
    df_name_col::String,
    df_source_col::String,
    table_destination_field::String,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = CSV.read(simulate_trial(), DataFrame); add_measurement_dates!(df, measurement_dates=measurement_dates)
    # table = "measurements"
    # df_name_col = "measurements"
    # df_source_col = "dates"
    # table_destination_field = "measure_date"
    # verbose::Bool = true
    check(conn, table)
    check(conn, table, "name")
    check(conn, table, table_destination_field)
    if df_name_col∉names(df)
        error(
            "The \"$df_name_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    if df_source_col∉names(df)
        error(
            "The \"$df_source_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    try
        check_illegal_strings(String.(unique(df[!, df_name_col])))
    catch e
        new_error = join(["Illegal string in the \"$df_name_col\" column!\n", sprint(showerror, e)])
        error(new_error)
    end
    if eltype(df[!, df_source_col]) <: AbstractString
        try
            check_illegal_strings(String.(unique(df[!, df_source_col])))
        catch e
            new_error = join(["Illegal string in the \"$df_source_col\" column!\n", sprint(showerror, e)])
            error(new_error)
        end
    end
    table_exists = nrow(DataFrame(execute(
        conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = \$1
        """,
        [table],
    ))) > 0
    if !table_exists
        error("The \"$table\" table does not exist in the database!")
    end
    field_exists = nrow(DataFrame(execute(
        conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = \$1
        AND column_name = \$2
        """,
        [table, table_destination_field],
    ))) > 0
    if !field_exists
        error("The \"$table_destination_field\" field does not exist in the \"$table\" table!")
    end
    # We extract the ids if we need to update the ids from some related table, i.e. if we have the pattern "*_id" for the `table_destination_field`
    df_tmp = if split(table_destination_field, "_")[end] == "id"
        df_tmp = unique(select(df, [df_name_col, df_source_col]))
        root_table = join(split(table_destination_field, "_")[1:(end-1)], "_")
        root_table = if root_table == "entry"
            "entries"
        elseif root_table[end] != 's'
            root_table * "s"
        else
            root_table
        end
        ids = String[]
        for x in String.(string.(df_tmp[!, df_source_col]))
            # x = df_tmp[!, df_source_col][1]
            push!(ids, execute(conn, "SELECT id FROM $root_table WHERE name = \$1", [x]) |> DataFrame |> x -> first(x.id))
        end
        df_tmp[!, df_source_col] = ids
        df_tmp
    else
        unique(select(df, [df_name_col, df_source_col]))
    end
    counter = 0
    pb = ProgressMeter.Progress(
        nrow(df_tmp),
        desc = "Updating $(nrow(df_tmp)) values of the \"$table_destination_field\" field in the \"$table\" table at ...",
    )
    execute(conn, "BEGIN")
    try
        bool = execute(conn, "SELECT EXISTS ( SELECT 1 FROM $table)") |> DataFrame |> x -> x.exists[1]
        if !bool
            error(
                "The \"$table\" table is empty! Please populate the \"name\" field first before updating the other fields using the \"name\" field.",
            )
        end
        for i = 1:nrow(df_tmp)
            # i = 1
            res = execute(
                conn,
                """
                UPDATE $table
                SET
                    $table_destination_field = \$1,
                    updated_at = now()
                WHERE name = \$2
                ;
                """,
                [df_tmp[i, df_source_col], df_tmp[i, df_name_col]],
            )
            # Note that the schema also automatically updates the `updated_at` field in the meta data tables (excludes `phenotype_data` and `environment_data` tables)
            if LibPQ.num_affected_rows(res) != 1
                error(
                    "Unexepcted number of rows affected, i.e. affecting $(LibPQ.num_affected_rows(res)) rows in \"$table\"!",
                )
            end
            counter += 1
            verbose ? ProgressMeter.next!(pb) : nothing
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Updated $counter rows in the \"$table\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end
