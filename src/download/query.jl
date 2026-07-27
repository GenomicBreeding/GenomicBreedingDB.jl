"""
    query(
        conn::LibPQ.Connection,
        filters::Vector{Filter};
        output_fields::Vector{String}=["*"],
        verbose::Bool=false,
    )::DataFrame

Query a database table using one or more validated filters and return the results
as a `DataFrame`.

The function executes a parameterised SQL query constructed from a collection of
`Filter` objects. All filters must reference the same table and are combined into
a single query expression. Selected records are returned as a `DataFrame`, with
foreign-key identifier fields automatically translated into their corresponding
human-readable names wherever possible.

By default, all columns are returned. A subset of columns may be requested using
the `output_fields` argument.

Missing foreign-key values are preserved as missing values in the output and are
excluded from identifier lookups, allowing queries to operate correctly on
tables containing incomplete relationships.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filters defining the query
  criteria. All filters must reference the same database table.
- `output_fields::Vector{String}=["*"]`: Fields to return in the query result.
  Use `["*"]` to return all fields.
- `verbose::Bool=false`: If `true`, display progress information whilst
  processing query results.

# Returns

- `DataFrame`: Records matching the supplied filters.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If the supplied filters fail validation.
- `ErrorException`: If an inferred lookup table name contains illegal
  characters.
- Any database exception raised whilst constructing or executing the query.

# Notes

- Connection validation is performed using `check(conn)`.
- Filter validation is performed using `check(filters)`.
- SQL clauses and query parameters are generated using `concat_filters`.
- All filtering is performed using parameterised SQL statements.
- Multiple filters are combined using logical `AND` conditions.
- By default, all columns are returned using `SELECT *`.
- Foreign-key fields ending in `_id` are automatically converted into the
  corresponding entity names.
- Identifier translation is performed by querying the corresponding lookup
  tables:
  - `entry_id` → `entries`
  - `species_id` → `species`
  - `<name>_id` → `<name>s`
- Missing identifier values are excluded from lookup queries and remain missing
  in the returned `DataFrame`.
- Converted fields are renamed by removing the `_id` suffix.
- The resulting `DataFrame` contains human-readable values rather than internal
  database identifiers wherever possible.
- When `verbose=true`, progress information is displayed while identifier fields
  are being resolved.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> df = query(conn, filters);

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

julia> df = query(conn, filters);

julia> prod(.!isnothing.(match.(Regex("site_1|site_2"), df.site))) == 1
true

julia> prod((df.value .>= 10) .&& (df.value .<= 20))
true

julia> df == query(conn, unique(filters))
true

julia> close(conn);
```
"""
function query(
    conn::LibPQ.Connection,
    filters::Vector{Filter};
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
    check(filters)
    table = filters[1].table
    filter_cat, par = concat_filters(filters, verbose = verbose)
    sql = join(vcat(String["SELECT $(join(output_fields, ',')) FROM $table WHERE 1=1"], filter_cat), " ")
    df = execute(conn, sql, par) |> DataFrame
    pb = ProgressMeter.Progress(ncol(df), desc = "Converting *_id fields into names...")
    for f in names(df)
        # f = names(df)[3]
        # println(f)
        isnothing(match(Regex("_id\$"), f)) ? continue : nothing
        f = replace(f, Regex("_id\$") => "")
        metatable = f == "entry" ? "entries" : f == "species" ? "species" : "$(f)s"
        check_illegal_strings([metatable])
        values = df[!, "$(f)_id"]
        df_tmp =
            execute(
                conn,
                "SELECT id,name FROM $metatable WHERE id = ANY(\$1)",
                [filter(xi -> !ismissing(xi), unique(values))],
            ) |> DataFrame
        for i = 1:nrow(df_tmp)
            # i = 1
            idx = findall(.!ismissing.(values) .&& (values .== df_tmp.id[i]))
            df[idx, "$(f)_id"] .= df_tmp.name[i]
        end
        rename!(df, "$(f)_id" => f)
        verbose ? ProgressMeter.next!(pb) : nothing
    end
    verbose ? ProgressMeter.finish!(pb) : nothing
    df
end
