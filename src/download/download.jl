"""
    query_table(
        conn::LibPQ.Connection;
        filters::Vector{Filter},
        output_fields::Vector{String} = ["*"],
        exclude_fields::Vector{String} = ["id", "created_at", "updated_at"],
        verbose::Bool = false,
    )::DataFrame

Query a database table using one or more filtering criteria.

The function dynamically constructs and executes a SQL query based on a
collection of `Filter` objects. Results can be restricted to selected
fields, and specified fields may be excluded from the final output.

After retrieving the query results, columns ending in `_id` are
automatically converted from database identifiers to their corresponding
names by querying the appropriate lookup table. Converted columns are
renamed by removing the `_id` suffix.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filtering criteria.
  All filters must reference the same database table, which is used as
  the query source table.
- `output_fields::Vector{String}=["*"]`: Fields to include in the query.
  Use `["*"]` to select all fields.
- `exclude_fields::Vector{String}=["id", "created_at", "updated_at"]`:
  Fields to remove from the returned `DataFrame`.
- `verbose::Bool=false`: If `true`, display progress bars and status
  messages during query construction and post-processing.

# Supported Filters

Each `Filter` object must define exactly one filtering condition:

- `filter_like` (one string no need for wildcards): Case-insensitive pattern matching (`ILIKE`).
- `filter_in` (one or more strings or numbers): Membership in a set of values (`IN`).
- `filter_between` (two numbers): Inclusive range filtering (`BETWEEN`).
- `filter_equal_to` (one number): Equality to a single value (`=`).
- `filter_less_than` (one number): Less-than comparison (`<`).
- `filter_greater_than` (one number): Greater-than comparison (`>`).

Multiple filters are combined using logical `AND`.

# Identifier Resolution

Columns whose names end in `_id` are automatically converted into
human-readable names where possible. For example:

- `entry_id` → `entry`
- `site_id` → `site`
- `program_id` → `program`

This is achieved by querying the corresponding lookup table and
replacing identifier values with the associated `name` values.

# Returns

- `DataFrame`: Query results after application of field exclusions and
  automatic identifier-to-name conversion.

# Throws

- An exception if the supplied filters reference different table names.
- An exception if the table name defined in the supplied filters, or any
  element of `output_fields`, contains illegal characters.
- An exception if a `Filter` object does not define a filtering
  condition.
- Any exception raised by PostgreSQL while executing the generated query.

# Notes

- All supplied filters must reference the same table. The query table is
  inferred from `filters`, and providing filters from multiple tables
  results in an error.
- Duplicate filters are automatically removed before query
  construction. Consequently, supplying the same `Filter` more than
  once has no effect on the query results.
- Supplied filters are combined using logical `AND`.
- Queries are parameterised to reduce the risk of SQL injection.
- Automatic `_id` conversion may require additional database queries and
  can increase execution time for large result sets.
- Fields listed in `exclude_fields` are removed after the query result
  has been retrieved.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="_01"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> df = query_table(conn, filters=filters);

julia> prod(.!isnothing.(match.(Regex("_01"), df.entry))) == 1
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
    exclude_fields::Vector{String} = ["id", "created_at", "updated_at"],
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
    # exclude_fields = ["id", "created_at", "updated_at"]
    # verbose = true
    validate_filters(filters)
    table = filters[1].table
    filter_cat, par = concat_filters(filters, verbose = verbose)
    sql = join(vcat(String["SELECT $(join(output_fields, ',')) FROM $table WHERE 1=1"], filter_cat), " ")
    df = execute(conn, sql, par) |> DataFrame
    select!(df, Not(exclude_fields))
    pb = ProgressMeter.Progress(ncol(df), desc = "Converting *_id fields into names...")
    for f in names(df)
        # f = names(df)[6]
        # f == "id" ? continue : nothing
        isnothing(match(Regex("_id\$"), f)) ? continue : nothing
        f = replace(f, Regex("_id\$") => "")
        metatable = f == "entry" ? "entries" : "$(f)s"
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
