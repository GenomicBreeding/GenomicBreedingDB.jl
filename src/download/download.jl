"""
    extract_all_tables(
        conn::LibPQ.Connection,
    )::DataFrame

List all user-defined database tables together with their estimated row counts.

The function validates that the supplied database connection is open and then
queries PostgreSQL system statistics to retrieve the names of all user tables and
their corresponding estimated number of live rows. Results are returned as a
sorted `DataFrame`.

This function is useful for inspecting the current database contents, verifying
that expected tables exist, and obtaining a quick overview of table sizes without
executing potentially expensive row-count queries.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.

# Returns

- `DataFrame`: Table containing the fields `table_name` and
  `estimated_row_count`.

# Throws

- `ErrorException`: If the database connection has been closed.
- Any database exception raised while querying PostgreSQL system statistics.

# Notes

- Connection validation is performed using `check(conn)`.
- Table information is obtained from PostgreSQL's `pg_stat_user_tables`
  system view.
- Row counts are estimates based on database statistics and may not exactly match
  the result of `COUNT(*)`.
- Only user-defined tables are included in the output.
- Results are sorted prior to being returned.
- The function performs a read-only query and does not modify the database.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> extract_all_tables(conn) |> nrow > 0
true

julia> close(conn);
```
"""
function extract_all_tables(conn::LibPQ.Connection)::DataFrame
    # conn = dbconnect()
    check(conn)
    execute(
        conn,
        """
        SELECT 
            relname AS table_name, 
            n_live_tup AS estimated_row_count
        FROM 
            pg_stat_user_tables
        """,
    ) |> DataFrame |> sort
end

"""
    extract_table_contents(
        conn::LibPQ.Connection,
        table::String,
    )::DataFrame

Extract all records from a database table and return them as a `DataFrame`.

The function validates that the supplied database connection is open and confirms
that the specified table exists before executing a `SELECT *` query. All rows and
columns from the table are retrieved and returned without modification.

This function provides a convenient way to inspect, export, or explore the
contents of a database table in tabular form.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table to extract.

# Returns

- `DataFrame`: Complete contents of the specified database table.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If the specified table does not exist.
- Any database exception raised whilst executing the query.

# Notes

- Connection validation is performed using `check(conn)`.
- Table validation is performed using `check(conn, table)`.
- The query retrieves all rows and all columns using `SELECT *`.
- No filtering, sorting, or column selection is applied.
- The function performs a read-only operation and does not modify the database.
- Large tables may require substantial memory to load into a `DataFrame`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> extract_table_contents(conn, "entries") |> nrow > 0
true

julia> close(conn);
```
"""
function extract_table_contents(conn::LibPQ.Connection, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"
    check(conn)
    check(conn, table)
    execute(conn, "SELECT * FROM $table") |> DataFrame
end

"""
    query_table(
        conn::LibPQ.Connection;
        filters::Vector{Filter},
        output_fields::Vector{String}=["*"],
        verbose::Bool=false,
    )::DataFrame

Query a database table using a collection of `Filter` objects and return the
results as a `DataFrame`.

The target table is inferred from the first filter in `filters`. All supplied
filters are validated and combined into a parameterised SQL `WHERE` clause. The
resulting query is executed and returned as a `DataFrame`.

Columns whose names end with `_id` are automatically resolved to their
corresponding entity names by querying the associated lookup table. Identifier
columns are replaced with the resolved names and renamed accordingly. For example,
`entry_id` becomes `entry`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filters used to construct the `WHERE`
  clause.
- `output_fields::Vector{String}=["*"]`: Columns to include in the `SELECT`
  statement.
- `verbose::Bool=false`: If `true`, display progress information while resolving
  foreign-key fields.

# Returns

- `DataFrame`: Query results with foreign-key identifiers resolved to their
  corresponding names.

# Notes

- Connection validation is performed using `check(conn)`.
- All filters must reference the same table.
- SQL parameters are supplied separately from the query text to support safe,
  parameterised execution.
- Foreign-key fields are identified using the `_id` suffix convention.
- Associated lookup tables are inferred automatically from field names, for
  example `site_id` → `sites`.
- The special case `entry_id` is resolved using the `entries` table.
- Progress reporting is available when `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> df = query_table(conn, filters=filters);

julia> prod(.!isnothing.(match.(Regex("1"), df.entry))) == 1
true

julia> prod(.!isnothing.(match.(Regex("site_1|site_2"), df.site))) == 1
true

julia> prod((df.value .>= 10) .&& (df.value .<= 20))
true

julia> table = "environment_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> df = query_table(conn, filters=filters);

julia> prod(.!isnothing.(match.(Regex("site_1|site_2"), df.site))) == 1
true

julia> prod((df.value .>= 10) .&& (df.value .<= 20))
true

julia> df == query_table(conn, filters=unique(filters))
true

julia> close(conn);
```
"""
function query_table(
    conn::LibPQ.Connection;
    filters::Vector{Filter},
    output_fields::Vector{String} = ["*"],
    verbose::Bool = false,
)::DataFrame
    # conn = dbconnect()
    # filters = [
    #     Filter(conn, table="phenotype_data", field="entry", filter_like="_01"),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    #     Filter(conn, table="phenotype_data", field="site", filter_in=["site_1", "site_2"]),
    # ]
    # output_fields = String["*"]
    # verbose = true
    check(conn)
    validate_filters(filters)
    table = filters[1].table
    filter_cat, par = concat_filters(filters, verbose = verbose)
    sql = join(vcat(String["SELECT $(join(output_fields, ',')) FROM $table WHERE 1=1"], filter_cat), " ")
    df = execute(conn, sql, par) |> DataFrame
    pb = ProgressMeter.Progress(ncol(df), desc = "Converting *_id fields into names...")
    for f in names(df)
        # f = names(df)[6]
        # f == "id" ? continue : nothing
        isnothing(match(Regex("_id\$"), f)) ? continue : nothing
        f = replace(f, Regex("_id\$") => "")
        metatable = f == "entry" ? "entries" : f == "species" ? "species" : "$(f)s"
        values = df[!, "$(f)_id"]
        df_tmp =
            execute(conn, "SELECT id,name FROM $metatable WHERE id = ANY(\$1)", [string.(unique(values))]) |> DataFrame
        df[!, "$(f)_id"] = [df_tmp.name[findfirst(df_tmp.id .== x)] for x in values]
        rename!(df, "$(f)_id" => f)
        verbose ? ProgressMeter.next!(pb) : nothing
    end
    verbose ? ProgressMeter.finish!(pb) : nothing
    df
end
