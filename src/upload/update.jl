"""
    update_table!(
        conn::LibPQ.Connection;
        filters::Vector{Filter},
        destination_field::String,
        value::Union{String,AbstractFloat,Int},
        verbose::Bool=false,
    )::Nothing

Update a single field in exactly one database record.

The function applies a parameterised SQL `UPDATE` statement to a database table
using a collection of validated filters to identify the target record. The
specified field is updated to the supplied value, and the operation succeeds only
when exactly one row is affected.

All updates are performed within a transaction. If the update affects zero rows
or more than one row, the transaction is rolled back and an error is raised to
prevent unintended modifications.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filters defining the target record.
  All filters must reference the same database table.
- `destination_field::String`: Name of the field to update.
- `value::Union{String,AbstractFloat,Int}`: New value to assign to the target
  field.
- `verbose::Bool=false`: If `true`, display progress information while
  concatenating filters.

# Returns

- `Nothing`: The specified field is updated in the database.

# Throws

- `ErrorException`: If the supplied filters reference multiple tables.
- `ErrorException`: If the table name contains illegal characters or strings.
- `ErrorException`: If the destination field contains illegal characters or
  strings.
- `ErrorException`: If the update affects zero rows.
- `ErrorException`: If the update affects more than one row.
- Any database exception raised whilst executing the update statement.

# Notes

- Filter validation is performed using `validate_filters`.
- SQL filter clauses and parameters are generated using `concat_filters`.
- Table and destination-field names are explicitly validated using
  `check_illegal_strings` before SQL construction.
- Updates are performed using parameterised SQL statements.
- The operation is wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Exactly one row must be updated for the operation to succeed.
- Updates affecting zero rows typically indicate that no record matched the
  supplied filters.
- Updates affecting multiple rows typically indicate that the supplied filters
  were insufficiently specific.
- This function is intended for targeted record updates rather than bulk
  modifications.

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
    # fname_reference_genome="Milnesium_tardigradum.fa"; simulate_reference_genome(fname_reference_genome=fname_reference_genome); upload_reference_genome!(conn, fname=fname_reference_genome, name="Milnesium tardigradum", notes="Simulated reference genome")
    # filters = [Filter(conn, table="reference_genomes", field="name", filter_in=["Milnesium tardigradum"])]
    # destination_field = "name"
    # value::Union{String, AbstractFloat, Int} = "some_new_name"
    # verbose = true
    validate_filters(filters)
    table = filters[1].table
    filter_cat, par = concat_filters(filters, verbose = verbose) # checks for illegal strings via early check(...) calls
    check_illegal_strings([table, destination_field]) # redundant but explicit checks for illegal strings before string interpolation below just to be extra safe
    sql = join(vcat(String["UPDATE $table SET $(destination_field) = \$$(length(par)+1) WHERE 1=1"], filter_cat), " ")
    execute(conn, "BEGIN")
    res = execute(conn, sql, vcat(par, value))
    if LibPQ.num_affected_rows(res) != 1
        execute(conn, "ROLLBACK")
        error(
            "Unexpected number of rows affected, i.e. affecting $(LibPQ.num_affected_rows(res)) rows in \"$table\"! The table may be empty or the filters yielded no or multiple matches!",
        )
    end
    execute(conn, "COMMIT")
    # execute(conn, "SELECT * FROM $table") |> DataFrame
    # query_table(conn, filters=filters)
    nothing
end

"""
    update_table_field_by_name!(
        conn::LibPQ.Connection;
        df::DataFrame,
        table::String,
        df_name_col::String,
        df_source_col::String,
        table_destination_field::String,
        verbose::Bool=false,
    )::Nothing

Update a field in a database table by matching records using their `name` values.

The function uses values from a DataFrame to update an existing database table. Rows
are matched using the table's `name` field and values from `df_name_col`. The
corresponding values from `df_source_col` are then written to
`table_destination_field`.

If the destination field represents a foreign-key relationship, identified by a
field name ending in `_id`, source values are automatically resolved to their
corresponding identifiers in the related table before updates are applied.

Updates are performed individually using `update_table!`, which ensures that each
update affects exactly one database record.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing source values for the update.
- `table::String`: Name of the table to update.
- `df_name_col::String`: Column containing names used to match records in the
  database table.
- `df_source_col::String`: Column containing values to write to the destination
  field.
- `table_destination_field::String`: Name of the field to update in the target
  table.
- `verbose::Bool=false`: If `true`, display progress information and summary
  messages during the update process.

# Returns

- `Nothing`: Records are updated directly in the database.

# Throws

- `ErrorException`: If the target table does not exist.
- `ErrorException`: If the table does not contain a `name` field.
- `ErrorException`: If the destination field does not exist.
- `ErrorException`: If either source column is missing from the DataFrame.
- `ErrorException`: If the target table is empty.
- Any database exception raised during the update process.

# Notes

- The target table must contain a `name` field used to identify records.
- Duplicate combinations of `df_name_col` and `df_source_col` are removed before
  processing.
- Destination fields ending in `_id` are interpreted as foreign keys and are
  resolved using `extract_ids`.
- Related table names are inferred automatically from the destination field name.
- `Date` and `DateTime` values are converted to strings before being written to the
  database.
- Updates are delegated to `update_table!`, which validates that exactly one record
  is modified for each operation.
- Progress reporting is available when `verbose=true`.

# Examples

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
    nothing
end
