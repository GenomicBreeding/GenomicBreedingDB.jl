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
the `output_fields` argument. For `phenotype_data` and `environment_data`
tables, user-facing field names are automatically converted into their
corresponding database foreign-key fields before query execution, allowing users
to request columns such as `entry`, `site`, `trait`, or
`environmental_variable` directly.

Missing foreign-key values are preserved as missing values in the output and are
excluded from identifier lookups, allowing queries to operate correctly on
tables containing incomplete relationships.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `filters::Vector{Filter}`: Collection of filters defining the query
  criteria. All filters must reference the same database table.
- `output_fields::Vector{String}=["*"]`: Fields to return in the query result.
  Use `["*"]` to return all fields.
- `verbose::Bool=false`: If `true`, display progress and status information
  during query construction, execution, and result processing.

# Returns

- `DataFrame`: Records matching the supplied filters.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If the supplied filters fail validation.
- `ErrorException`: If any supplied output field contains illegal characters or
  strings.
- `ErrorException`: If an inferred lookup table name contains illegal
  characters.
- Any database exception raised whilst constructing or executing the query.

# Notes

- Connection validation is performed using `check(conn)`.
- Filter validation is performed using `check(filters)`.
- Requested output fields are validated using `check_illegal_strings`.
- SQL clauses and query parameters are generated using `concat_filters`.
- All filtering is performed using parameterised SQL statements.
- Multiple filters are combined using logical `AND` conditions.
- By default, all columns are returned using `SELECT *`.
- When querying `phenotype_data` or `environment_data`, requested fields are
  automatically translated into the underlying database schema:
  - `id`
  - `value`
  - `created_at`
  - `updated_at`
  are included directly.
- All other requested fields are translated to their corresponding foreign-key
  columns by appending `_id` before query execution.
- Foreign-key fields ending in `_id` are automatically resolved back into
  human-readable names after query execution.
- Identifier translation is performed by querying the corresponding lookup
  tables:
  - `entry_id` → `entries`
  - `species_id` → `species`
  - `<name>_id` → `<name>s`
- Missing identifier values are excluded from lookup queries and remain missing
  in the returned `DataFrame`.
- Converted columns are renamed by removing the `_id` suffix.
- The resulting `DataFrame` contains human-readable values rather than internal
  database identifiers wherever possible.
- When `verbose=true`, status messages are displayed during query construction
  and execution, and progress information is displayed while identifier fields
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

julia> df_new = query(conn, filters, output_fields=["experiment", "treatment", "environment_variable", "value"]);

julia> select(df, ["experiment", "treatment", "environment_variable", "value"]) == df_new
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
    output_fields != ["*"] ? check_illegal_strings(output_fields) : nothing
    output_fields = if (output_fields != ["*"]) && ((table == "phenotype_data") || (table == "environment_data"))
        tmp = deepcopy(output_fields)
        for i = 1:length(output_fields)
            # i = 1
            f = output_fields[i]
            if (f == "id") || (f == "value") || (f == "created_at") || (f == "updated_at")
                continue
            end
            tmp[i] = string(f, "_id")
        end
        tmp
    else
        output_fields
    end
    verbose ? println("Concatenating the filters and buidling the query statement...") : nothing
    filter_cat, par = concat_filters(filters, verbose = verbose)
    sql = join(vcat(String["SELECT $(join(output_fields, ',')) FROM $table WHERE 1=1"], filter_cat), " ")
    verbose ? println("Querying the database...") : nothing
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
    if verbose
        ProgressMeter.finish!(pb)
        println("Done!")
    end
    df
end

"""
    query(
        conn::LibPQ.Connection,
        args::AbstractDict{String};
        table::String,
        expected_fields::Vector{String}=["*"],
        backup_field_string::Union{Nothing,String}=nothing,
        backup_field_numeric::Union{Nothing,String}=nothing,
        verbose::Bool=false,
    )::DataFrame

Construct and execute a database query from a collection of filtering
arguments.

The function translates user-supplied filtering arguments into validated
`Filter` objects and executes the resulting query against the specified
database table. Argument names are automatically mapped to database field
names, filtered against an optional set of permitted fields, and converted
into the appropriate filter types.

Arguments prefixed with `like_` are interpreted as pattern-matching filters
and converted into `filter_like` constraints. The special field `value` is
interpreted as a numeric interval and converted into a `filter_between`
constraint. All other recognised fields are interpreted as exact-match filters
and converted into `filter_in` constraints.

All generated filters are validated, type-checked, sanitised, and translated
into parameterised SQL query components via the `Filter` constructor before
being executed by the lower-level `query(conn, filters; ...)` method.

If no valid filters are generated, a fallback filter is automatically created
using either a string or numeric field, allowing unrestricted queries whilst
maintaining compatibility with APIs that require at least one `Filter`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `args::AbstractDict{String}`: Collection of filtering arguments.
- `table::String`: Name of the target database table.
- `expected_fields::Vector{String}=["*"]`: Fields permitted to participate in
  query construction. Use `["*"]` to allow all supplied fields.
- `backup_field_string::Union{Nothing,String}=nothing`: String field used to
  generate a catch-all filter when no valid filters are supplied.
- `backup_field_numeric::Union{Nothing,String}=nothing`: Numeric field used to
  generate a catch-all filter when no valid filters are supplied and no string
  fallback field is available.
- `verbose::Bool=false`: If `true`, display progress information during query
  execution.

# Returns

- `DataFrame`: Records matching the generated filtering criteria.

# Throws

- `ErrorException`: If no valid filters are generated and neither
  `backup_field_string` nor `backup_field_numeric` is supplied.
- Any exception raised whilst constructing `Filter` objects.
- Any exception raised during query execution.

# Notes

- Filter names are converted from plural to singular form where appropriate:
  - `traits` → `trait`
  - `entries` → `entry`
  - `species` → `species`
- Arguments prefixed with `like_` generate one `filter_like` constraint for
  each supplied value.
- The special field `value` is converted into a `filter_between` constraint.
- All other recognised fields are converted into `filter_in` constraints.
- Empty filter values are ignored.
- If `expected_fields != ["*"]`, only fields present in
  `expected_fields` are used to construct filters.
- All generated filters are validated against the database schema, checked for
  type compatibility, sanitised, and converted into parameterised SQL query
  fragments by the `Filter` constructor.
- If no valid filters are generated and `backup_field_string` is supplied, a
  catch-all filter of `backup_field_string LIKE '%'` is used.
- If no valid filters are generated and only
  `backup_field_numeric` is supplied, a catch-all filter of
  `backup_field_numeric > -1_000_000` is used.
- This function is intended as a convenience wrapper around `Filter` and
  `query`, reducing repetitive filter-construction logic in higher-level data
  extraction and download functions.
- Future extensions may support interval-based filtering for additional field
  types such as dates and datetimes.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try query(conn, Dict("like_entries"=>["010"]), table="entries"); catch; false; end
false

julia> df_ent1 = query(conn, Dict("like_name"=>["010"]), table="entries");

julia> df_ent2 = query(conn, Dict("like_names"=>["010"]), table="entries");

julia> (nrow(df_ent1) == nrow(df_ent2)) && (nrow(df_ent1) > 0)
true

julia> df_phe = query(conn, Dict("like_entries"=>["010"]), table="phenotype_data");

julia> nrow(df_phe) > 0
true

julia> df_lay = query(conn, Dict("replication"=>[1], "row"=>[1]), table="layouts");

julia> nrow(df_lay) > 0
true

julia> try query(conn, Dict("entries"=>["SOME_NAME_THAT_DOES_NOT_EXIST"]), table="phenotype_data"); catch; false; end
false

julia> try query(conn, Dict("entries"=>[]), table="phenotype_data"); catch; false; end
false

julia> df_nul = query(conn, Dict("entries"=>[]), table="phenotype_data", backup_field_numeric="value");

julia> nrow(df_nul) > 0
true

julia> close(conn);
```
"""
function query(
	conn::LibPQ.Connection,
	args::AbstractDict{String};
	table::String,
	expected_fields::Vector{String} = ["*"],
	backup_field_string::Union{Nothing, String} = nothing,
	backup_field_numeric::Union{Nothing, String} = nothing,
	verbose::Bool = false,
)::DataFrame
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        if (expected_fields != ["*"]) && (field ∉ expected_fields)
            continue
        end
		if is_like
			for vi in v
				push!(filters, Filter(conn, table = table, field = field, filter_like = vi))
			end
		elseif field == "value"
			push!(filters, Filter(conn, table = table, field = field, filter_between = v))
		else
			# TODO: probably for date intervals...
			push!(filters, Filter(conn, table = table, field = field, filter_in = v))
		end
    end
    filters = if (length(filters) == 0) && !isnothing(backup_field_string)
        push!(filters, Filter(conn, table = table, field = backup_field_string, filter_like = "%"))
    elseif (length(filters) == 0) && !isnothing(backup_field_numeric)
        push!(filters, Filter(conn, table = table, field = backup_field_numeric, filter_greater_than = -1_000_000))
	elseif length(filters) == 0
		error("Please define either `backup_field_string` or `backup_field_numeric`!")
	else
        filters
    end
    query(conn, filters, verbose = verbose)
end