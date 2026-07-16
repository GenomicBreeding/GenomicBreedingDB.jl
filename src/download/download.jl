"""
    query_table(
        conn::LibPQ.Connection;
        table::String,
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
- `table::String`: Name of the table to query.
- `filters::Vector{Filter}`: Collection of filtering criteria.
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

- An exception if `table` or any element of `output_fields` contains
  illegal characters.
- An exception if a `Filter` object does not define a filtering
  condition.
- Any exception raised by PostgreSQL while executing the generated query.

# Notes

- Duplicate filters are automatically removed before query
  construction. Consequently, supplying the same `Filter` more than
  once has no effect on the query results:
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

julia> df = query_table(conn, table=table, filters=filters);

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

julia> df = query_table(conn, table=table, filters=filters);

julia> prod(.!isnothing.(match.(Regex("site_1|site_2"), df.site))) == 1
true

julia> prod((df.value .>= 10) .&& (df.value .<= 20))
true

julia> df == query_table(conn, table=table, filters=unique(filters))
true

julia> close(conn);
```
"""
function query_table(
    conn::LibPQ.Connection;
    table::String,
    filters::Vector{Filter},
    output_fields::Vector{String} = ["*"],
    exclude_fields::Vector{String} = ["id", "created_at", "updated_at"],
    verbose::Bool = false,
)::DataFrame
    # conn = dbconnect()
    # table = "phenotype_data"
    # filters = [
    #     Filter(conn, table="phenotype_data", field="entry", filter_like="_01"),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    #     Filter(conn, table="phenotype_data", field="site", filter_in=["site_1", "site_2"]),
    # ]
    # output_fields = String["*"]
    # exclude_fields = ["id", "created_at", "updated_at"]
    # verbose = true
    if output_fields == String["*"]
        check_illegal_strings([table])
    else
        check_illegal_strings(vcat([table], output_fields))
    end
    filters_counts = countmap(filters)
    filters = if length(filters_counts) == length(filters)
        filters
    else
        verbose ?
        warn("Duplicate filters found: \n\t- $(join(filter(x -> x[2] > 1, filters_counts), "; \n\t- "))") : nothing
        unique(filters)
    end
    sql = String["SELECT $(join(output_fields, ',')) FROM $table WHERE 1=1"]
    par = String[]
    pb = ProgressMeter.Progress(length(filters), desc = "Defining the query statement...")
    for f in filters
        # f = filters[1]
        n = length(par)
        if !isnothing(f.like)
            push!(sql, "AND $(f.field) ILIKE $(f.like)")
            append!(par, String(f.like))
        elseif !isnothing(f.in)
            s = "($(join(string.("\$", (n+1):(n+length(f.in))), ',')))"
            push!(sql, "AND $(f.field) IN $s") # why not just use ANY? Because we have potentially more than one filter and LibPQ does not seem to allow me to use parameters with individual elements and vectors, hence multiple parameters and LibPQ does not seem
            append!(par, string.(f.in))
        elseif !isnothing(f.between)
            push!(sql, "AND $(f.field) BETWEEN \$$(n+1) AND \$$(n+2)")
            append!(par, string.([f.between[1], f.between[2]]))
        elseif !isnothing(f.equal_to)
            push!(sql, "$(f.field) = \$$(n+1)")
            append!(par, String(f.equal_to))
        elseif !isnothing(f.less_than)
            push!(sql, "$(f.field) < \$$(n+1)")
            append!(par, String(f.less_than))
        elseif !isnothing(f.greater_than)
            push!(sql, "$(f.field) > \$$(n+1)")
            append!(par, String(f.greater_than))
        else
            error("No filtering defined in $f.")
        end
        verbose ? ProgressMeter.next!(pb) : nothing
    end
    if verbose
        ProgressMeter.finish!(pb)
        println("Querying...")
    end
    sql = join(sql, " ")
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
