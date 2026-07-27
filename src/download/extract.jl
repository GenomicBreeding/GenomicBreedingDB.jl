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
    extract_ids(
        conn::LibPQ.Connection;
        names::Vector{String},
        table::String,
        is_like::Bool=false,
    )::DataFrame

Retrieve identifier and name pairs from a database table for a collection of names.

When `is_like` is `false`, names are matched exactly using a SQL `ANY` condition.
When `is_like` is `true`, each provided name is matched using a case-insensitive
`ILIKE` search, allowing partial matches. Matching records are returned as a
`DataFrame` containing `id` and `name` columns.

The function validates that the specified table exists and contains a `name` column
before executing any queries. Missing identifiers are removed from the result and
all returned values are converted to `String`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `names::Vector{String}`: Names to search for in the specified table.
- `table::String`: Name of the table containing `id` and `name` fields.
- `is_like::Bool=false`: If `true`, perform case-insensitive partial matching using
  `ILIKE`; otherwise perform exact matching.

# Returns

- `DataFrame`: Table containing matching `id` and `name` pairs.

# Notes

- The specified table must contain both `id` and `name` columns.
- Partial matching is performed individually for each element in `names`.
- Underscore characters in search strings are escaped when using `ILIKE` to prevent
  unintended wildcard matching.
- Rows with missing identifiers are removed from the returned result.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_extract_ids_$(Dates.time() |> x -> replace("$x", "." => "_"))-", df.entries);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> df_entries = extract_ids(conn, names=sort(unique(df.entries)), table="entries");

julia> sort(df_entries.name) == sort(unique(df.entries))
true

julia> close(conn);
```
"""
function extract_ids(conn::LibPQ.Connection; names::Vector{String}, table::String, is_like::Bool = false)::DataFrame
    # conn = dbconnect(); names = String["entry_001", "entry_004"]; table = "entries"; is_like = true
    check(conn, table)
    check(conn, table, "id")
    check(conn, table, "name")
    df = if !is_like
        DataFrame(execute(conn, "SELECT id,name FROM $table WHERE name = ANY(\$1)", [names]))
    else
        df = DataFrame(id = String[], name = String[])
        for name in names
            # name = names[1]
            df_tmp = DataFrame(
                execute(conn, "SELECT id,name FROM $table WHERE name ILIKE \$1", ["%$(replace(name, "_" => "\\_"))%"]),
            )
            df = vcat(df, df_tmp)
        end
        df
    end
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end

"""
    extract_names(
        conn::LibPQ.Connection;
        ids::Vector{String},
        table::String,
    )::DataFrame

Retrieve name values associated with a collection of identifiers from a database
table.

The function queries the specified table for records whose identifiers match the
provided `ids` and returns the corresponding `id` and `name` pairs as a
`DataFrame`. The table and required `name` column are validated before the query is
executed.

Rows with missing identifiers are removed from the result, and both `id` and `name`
columns are converted to `String` values before being returned.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `ids::Vector{String}`: Identifiers to look up in the specified table.
- `table::String`: Name of the table containing `id` and `name` fields.

# Returns

- `DataFrame`: Table containing matching `id` and `name` pairs.

# Notes

- The specified table must exist and contain `id` and `name` columns.
- Identifier matching is performed using a parameterised SQL `ANY` condition.
- Rows with missing identifiers are excluded from the returned result.
- Returned `id` and `name` values are converted to `String` for consistency.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_extract_names_$(Dates.time() |> x -> replace("$x", "." => "_"))-", df.entries);

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> df_entries_0 = extract_ids(conn, names=sort(unique(df.entries)), table="entries");

julia> df_entries_1 = extract_names(conn, ids=df_entries_0.id, table="entries");

julia> close(conn);

julia> df_entries_0 == df_entries_1
true
```
"""
function extract_names(conn::LibPQ.Connection; ids::Vector{String}, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"; ids = extract_ids(conn, names=String["entry_001", "entry_100"], table=table).id;
    check(conn, table)
    check(conn, table, "id")
    check(conn, table, "name")
    df = DataFrame(execute(conn, "SELECT id,name FROM $table WHERE id = ANY(\$1)", [ids]))
    filter!(x -> !ismissing(x.id), df)
    df.id .= String.(df.id)
    df.name .= String.(df.name)
    df
end
