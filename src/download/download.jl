"""
    Filter

Represents a validated query filter for a database table column.

A `Filter` encapsulates a single filtering operation on a field within a
database table. Exactly one filtering criterion must be supplied when
constructing a `Filter`.

# Fields

- `table::String`: Name of the target database table.
- `field::String`: Name of the target field (column).
- `like::Union{Nothing,String}`: Pattern used for fuzzy matching.
- `in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}`:
  Values used in an SQL `IN (...)` clause.
- `between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}}`:
  Inclusive lower and upper bounds for range filtering.
- `equal_to::Union{Nothing,Int,AbstractFloat}`:
  Exact value match.
- `less_than::Union{Nothing,Int,AbstractFloat}`:
  Upper bound filter.
- `greater_than::Union{Nothing,Int,AbstractFloat}`:
  Lower bound filter.

# Constructor
    Filter(
        conn::LibPQ.Connection;
        table::String,
        field::String,
        filter_like::Union{Nothing, String} = nothing,
        filter_in::Union{Nothing, Vector{String}, Vector{Int}, Vector{AbstractFloat}} = nothing,
        filter_between::Union{Nothing, Tuple{Int, Int}, Tuple{AbstractFloat, AbstractFloat}} = nothing,
        filter_equal_to::Union{Nothing, Int, AbstractFloat} = nothing,
        filter_less_than::Union{Nothing, Int, AbstractFloat} = nothing,
        filter_greater_than::Union{Nothing, Int, AbstractFloat} = nothing,
    )

Create a validated database filter.

The constructor verifies that:

1. The specified `table` exists.
2. The specified `field` exists, or can be resolved to a corresponding
    foreign-key field (e.g. `entries → entry_id`).
3. Exactly one filtering criterion is supplied.
4. Identifier-based fields (`*_id`) can accept names and automatically
    resolve them to IDs.

# Arguments
- `conn::LibPQ.Connection`: Active database connection.
- `table::String`: Target table name.
- `field::String`: Target field name.
- `filter_like::Union{Nothing,String}`: Match records using a fuzzy search.
- `filter_in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}`: Match records whose values are contained in a collection.
- `filter_between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}}`: Match records with values between two bounds.
- `filter_equal_to::Union{Nothing,Int,AbstractFloat}`: Match records equal to a specific value.
- `filter_less_than::Union{Nothing,Int,AbstractFloat}`: Match records with values less than the specified value.
- `filter_greater_than::Union{Nothing,Int,AbstractFloat}`: Match records with values greater than the specified value.

# Details
- `filter_like` (one string no need for wildcards): Case-insensitive pattern matching (`ILIKE`).
- `filter_in` (one or more strings or numbers): Membership in a set of values (`IN`).
- `filter_between` (two numbers): Inclusive range filtering (`BETWEEN`).
- `filter_equal_to` (one number): Equality to a single value (`=`).
- `filter_less_than` (one number): Less-than comparison (`<`).
- `filter_greater_than` (one number): Greater-than comparison (`>`).

# Notes

For foreign-key fields ending in `_id`, string values supplied via
`filter_in` or `filter_like` are automatically translated to their
corresponding database identifiers using `extract_ids`.

Exactly one of the `filter_*` keyword arguments must be non-`nothing`.

# Throws

- `ErrorException` if the table does not exist.
- `ErrorException` if the field cannot be resolved.
- `ErrorException` if zero or multiple filtering criteria are supplied.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try Filter(conn, table="entries", field="name", filter_like="_010", filter_in="entry_100"); catch; false; end
false

julia> Filter(conn, table="entries", field="name", filter_like="_010")
Filter("entries", "name", "_010", nothing, nothing, nothing, nothing, nothing)

julia> x = Filter(conn, table="phenotype_data", field="entry", filter_like="entry_100");

julia> (x.table == "phenotype_data") && (x.field == "entry_id") && isnothing(x.like) && !isnothing(x.in)
true

julia> x = Filter(conn, table="phenotype_data", field="site", filter_in=["site_1", "site_2"]);

julia> (x.table == "phenotype_data") && (x.field == "site_id") && isnothing(x.like) && !isnothing(x.in)
true

julia> x = Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20));

julia> (x.table == "phenotype_data") && (x.field == "value") && (x.between == (10, 20))
true

julia> x = Filter(conn, table="phenotype_data", field="value", filter_equal_to=10);

julia> (x.table == "phenotype_data") && (x.field == "value") && (x.equal_to == 10)
true

julia> x = Filter(conn, table="phenotype_data", field="value", filter_less_than=10);

julia> (x.table == "phenotype_data") && (x.field == "value") && (x.less_than == 10)
true

julia> x = Filter(conn, table="phenotype_data", field="value", filter_greater_than=10);

julia> (x.table == "phenotype_data") && (x.field == "value") && (x.greater_than == 10)
true
```
"""
struct Filter
    table::String
    field::String
    like::Union{Nothing,String}
    in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}
    between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}}
    equal_to::Union{Nothing,Int,AbstractFloat}
    less_than::Union{Nothing,Int,AbstractFloat}
    greater_than::Union{Nothing,Int,AbstractFloat}
    function Filter(
        conn::LibPQ.Connection;
        table::String,
        field::String,
        filter_like::Union{Nothing,String} = nothing,
        filter_in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}} = nothing,
        filter_between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}} = nothing,
        filter_equal_to::Union{Nothing,Int,AbstractFloat} = nothing,
        filter_less_than::Union{Nothing,Int,AbstractFloat} = nothing,
        filter_greater_than::Union{Nothing,Int,AbstractFloat} = nothing,
    )
        # conn = dbconnect();
        # filter_like=nothing; filter_in=nothing; filter_between=nothing; filter_equal_to=nothing; filter_less_than=nothing; filter_greater_than=nothing;
        # table = "entries"; field = "name"; filter_in = String["entry_100"]; # table = "phenotype_data"; field = "entries"; filter_in = String["entry_100"]; # table = "phenotype_data"; field = "site"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "site_id"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "WQRERWE"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "site"; filter_like = "site"; # table = "phenotype_data"; field = "site"; # table = "phenotype_data"; field = "entry"; filter_in = String["entry_010", "entry_020"]; # table = "phenotype_data"; field = "entry"; filter_in = String["entry_010"]; # table = "phenotype_data"; field = "entry";
        # execute(conn, "SELECT id,value FROM phenotype_data") |> DataFrame
        # table = "phenotype_data"; field = "value"; filter_in = Float64[10.515928568077884]; # table = "phenotype_data"; field = "value"; filter_between = (10, 12); # table = "phenotype_data"; field = "value"; filter_equal_to = 10.515928568077884; # table = "phenotype_data"; field = "value"; filter_less_than = 10; # table = "phenotype_data"; field = "value"; filter_greater_than = 100
        check(conn, table)
        sum([
            !isnothing(filter_like),
            !isnothing(filter_in),
            !isnothing(filter_between),
            !isnothing(filter_equal_to),
            !isnothing(filter_less_than),
            !isnothing(filter_greater_than),
        ]) != 1 ? error("We expect one and only one `filter_*` argument!") : nothing
        field = try
            check(conn, table, field)
            field
        catch
            if field == "entries"
                "entry_id"
            else
                field_split = collect(field)
                if field_split[end] == 's'
                    "$(join(field_split[1:(end-1)]))_id"
                else
                    "$(field)_id"
                end
            end
        end
        check(conn, table, field)
        filter_in, filter_like = if isnothing(match(Regex("_id\$"), field))
            filter_in, filter_like
        else
            metatable = field == "entry_id" ? "entries" : replace(field, "_id" => "s")
            filter_in = isnothing(filter_in) ? nothing : extract_ids(conn, names = filter_in, table = metatable).id
            filter_like =
                isnothing(filter_like) ? nothing :
                extract_ids(conn, names = [filter_like], table = metatable, is_like = true).id
            if !isnothing(filter_like)
                # Here we set the `filter_like` into `filter_in` because we already assigned the query matches from above an no longer need to do fuzzy search
                (filter_like, nothing)
            else
                (filter_in, nothing)
            end
        end
        # !isnothing(filter_like) ? let sql = "SELECT * FROM $table WHERE $field LIKE ($(join(string.("\$", 1:length(filter_like)), ',')))"; execute(conn, sql, filter_like) |> DataFrame; end : nothing
        # !isnothing(filter_in) ? let sql = "SELECT * FROM $table WHERE $field IN ($(join(string.("\$", 1:length(filter_in)), ',')))"; execute(conn, sql, filter_in) |> DataFrame; end : nothing
        # !isnothing(filter_between) ? execute(conn, "SELECT id,value FROM $table WHERE $field != 'NaN' AND $field BETWEEN \$1 AND \$2", [filter_between[1], filter_between[2]]) |> DataFrame : nothing
        # !isnothing(filter_equal_to) ? execute(conn, "SELECT id,value FROM $table WHERE $field != 'NaN' AND $field = \$1", [filter_equal_to]) |> DataFrame : nothing
        # !isnothing(filter_less_than) ? execute(conn, "SELECT id,value FROM $table WHERE $field != 'NaN' AND $field < \$1", [filter_less_than]) |> DataFrame : nothing
        # !isnothing(filter_greater_than) ? execute(conn, "SELECT id,value FROM $table WHERE $field != 'NaN' AND $field > \$1", [filter_greater_than]) |> DataFrame : nothing
        new(
            table,
            field,
            filter_like,
            filter_in,
            filter_between,
            filter_equal_to,
            filter_less_than,
            filter_greater_than,
        )
    end
end

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

- All supplied filters are combined using logical `AND`.
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
    #     Filter(conn, table="phenotype_data", field="site", filter_in=["site_1", "site_2"]),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    # ]
    # output_fields = String["*"]
    # exclude_fields = ["id", "created_at", "updated_at"]
    # verbose = true
    if output_fields == String["*"]
        check_illegal_strings([table])
    else
        check_illegal_strings(vcat([table], output_fields))
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
