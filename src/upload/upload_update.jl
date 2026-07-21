"""
    update_table!(
        conn::LibPQ.Connection;
        filters::Vector{Filter},
        destination_field::String,
        value::Union{String,AbstractFloat,Int},
        verbose::Bool=false,
    )::Nothing

Update a single field in a database table for records matching a set of
filtering criteria.

The function constructs and executes a parameterised SQL `UPDATE`
statement using the supplied filters. All filters must reference the
same database table. The specified `destination_field` is updated to the
supplied `value` for records satisfying all filter conditions.

For safety, the function expects exactly one row to be modified. An
exception is raised if zero rows or multiple rows are affected.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filtering criteria used to
  identify the record to update. All filters must reference the same
  table.
- `destination_field::String`: Field to update.
- `value::Union{String,AbstractFloat,Int}`: New value to assign
  to `destination_field`.
- `verbose::Bool=false`: If `true`, display progress information while
  constructing the filtering clause.

# Returns

- `nothing` if the update succeeds and exactly one row is modified.

# Throws

- An exception if the filters reference multiple tables.
- An exception if any `Filter` does not define a filtering condition.
- An exception if the update affects a number of rows other than one.
- Any exception raised by PostgreSQL during query execution.

# Notes

- All supplied filters are combined using logical `AND`.
- The SQL statement is parameterised to reduce the risk of SQL
  injection.
- This function is intended for updating a single database record.
  Updates affecting multiple rows are treated as errors.
- Filter processing is delegated to `validate_filters()` and
  `concat_filters()`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> date_string = replace(string(time()), "." => "_");

julia> df.species .= string("Homo sapiens-", date_string);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="species", df_col="species");

julia> df_before_1 = LibPQ.execute(conn, "SELECT * FROM species WHERE name ILIKE '%Homo sapiens%'") |> DataFrame;

julia> df_before_2 = LibPQ.execute(conn, "SELECT * FROM species WHERE name ILIKE '%Homo habilis%'") |> DataFrame;

julia> filters = [Filter(conn, table="species", field="name", filter_in=[string("Homo sapiens-", date_string)])];

julia> update_table!(conn, filters=filters, destination_field="name", value=string("Homo habilis-", date_string));

julia> df_after_1 = LibPQ.execute(conn, "SELECT * FROM species WHERE name ILIKE '%Homo sapiens%'") |> DataFrame;

julia> df_after_2 = LibPQ.execute(conn, "SELECT * FROM species WHERE name ILIKE '%Homo habilis%'") |> DataFrame;

julia> (nrow(df_before_1) > nrow(df_after_1)) && (nrow(df_before_2) < nrow(df_after_2))
true

julia> close(conn);
```
"""
function update_table!(
    conn::LibPQ.Connection;
    filters::Vector{Filter},
    destination_field::String,
    value::Union{String,AbstractFloat,Int},
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # filters = [Filter(conn, table="reference_genomes", field="name", filter_in=["Milnesium tardigradum"])]
    # destination_field = "name"
    # value::Union{String, AbstractFloat, Int} = "some_new_name"
    # verbose = true
    validate_filters(filters)
    table = filters[1].table
    filter_cat, par = concat_filters(filters, verbose = verbose)
    sql = join(vcat(String["UPDATE $table SET $(destination_field) = \$$(length(par)+1) WHERE 1=1"], filter_cat), " ")
    res = execute(conn, sql, vcat(par, value))
    if LibPQ.num_affected_rows(res) != 1
        error(
            "Unexpected number of rows affected, i.e. affecting $(LibPQ.num_affected_rows(res)) rows in \"$table\"! The table may be empty or the filters yielded no matches!",
        )
    end
    # execute(conn, "SELECT * FROM $table") |> DataFrame
    # query_table(conn, filters=filters)
    nothing
end

"""
    update_table_field_by_name!(conn::LibPQ.Connection; df::DataFrame, table::String, df_name_col::String, df_source_col::String, table_destination_field::String, verbose::Bool = false)::Nothing

Update a specific field in a database table by matching records based on a name column in a DataFrame.

# Arguments
- `conn::LibPQ.Connection`: Active database connection.
- `df::DataFrame`: Source DataFrame containing the data to update.
- `table::String`: Name of the target table in the database.
- `df_name_col::String`: Name of the column in `df` whose values are matched against the `name` field of the target database table.
- `df_source_col::String`: Name of the column in `df` containing the values used to update `table_destination_field`. This may be the same as `df_name_col`.
- `table_destination_field::String`: Name of the field in the database table to update. If the field name ends with "_id", the function will resolve foreign key references from the corresponding related table.
- `verbose::Bool`: If `true`, displays a progress bar during the update operation. Defaults to `false`.

# Returns
- `Nothing`

# Behaviour
- Validates that `df_name_col` and `df_source_col` exist in the provided DataFrame.
- Supports `df_name_col == df_source_col`, allowing the same column to be used both for row matching and as the source of update values.
- Validates that the target table and field exist in the database.
- Validates string columns using `check_illegal_strings()` to ensure they contain only allowed characters for database identifiers and names. The following characters are not allowed: `;`, `|`, `,`, `.`, `/`, `\\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`, `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`, `?`. Non-ASCII characters are also rejected.
- Supports `df_name_col == df_source_col`, allowing the same column to be used both for row matching and as the source of update values.
- Removes duplicate update operations by working on unique combinations of matching and source values.
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
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_update_table-", time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> df.species .= string.("test_update_table-", time() |> x -> replace(string(x), "." => "_"), "-Elysia chlorotica");

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> insert_names!(conn, df=df, table="species", df_col="species");

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="species", table_destination_field="species_id");

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> sum(.!ismissing.(unique(df_before.species_id))) < sum(.!ismissing.(unique(df_after.species_id)))
true

julia> update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="entries", table_destination_field="name");

julia> df_before = deepcopy(df_after); df_after = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> sum(.!ismissing.(unique(df_before.name))) == sum(.!ismissing.(unique(df_after.name)))
true

julia> idx_before = findall(df_before.name .== df.entries[1]); idx_after = findall(df_after.name .== df.entries[1]);

julia> unique(df_before.updated_at[idx_before]) < unique(df_after.updated_at[idx_after]) # also shows that the update_at automatically updates
true

julia> close(conn);
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
    # df = simulate_genomes() |> simulate_trials |> tabularise
    # add_measurement_dates!(df)
    # table = "measurements"
    # df_name_col = "measurements"
    # df_source_col = "dates"
    # table_destination_field = "measure_date"
    # # table = "entries"
    # # add_col!(df, col = "species", value = "Acacia neglecta")
    # # df_name_col = "entries"
    # # df_source_col = "species"
    # # table_destination_field = "species_id"
    # verbose::Bool = true
    check(conn, table)
    check(conn, table, "name")
    check(conn, table, table_destination_field)
    check(df, df_name_col)
    check(df, df_source_col)
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
            push!(ids, extract_ids(conn, names = [x], table = root_table).id[1])
            # push!(ids, execute(conn, "SELECT id FROM $root_table WHERE name = \$1", [x]) |> DataFrame |> x -> first(x.id))
        end
        df_tmp[!, df_source_col] = ids
        df_tmp
    else
        unique(select(df, unique([df_name_col, df_source_col])))
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
        end # Making this explicit here, although this error is covered in update_table!(...), because I like it to be explicit here in this specific-use-case function...
        for i = 1:nrow(df_tmp)
            # i = 1
            value = if isa(df_tmp[i, df_source_col], Date) || isa(df_tmp[i, df_source_col], DateTime)
                String(string(df_tmp[i, df_source_col]))
            else
                df_tmp[i, df_source_col]
            end
            update_table!(
                conn,
                filters = [Filter(conn, table = table, field = "name", filter_in = String[df_tmp[i, df_name_col]])],
                destination_field = table_destination_field,
                value = value,
                verbose = verbose,
            )
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
