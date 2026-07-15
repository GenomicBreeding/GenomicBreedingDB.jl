function list_tables(conn::LibPQ.Connection)::DataFrame
    # conn = dbconnect()
    execute(
        conn, 
        """
        SELECT 
            relname AS table_name, 
            n_live_tup AS estimated_row_count
        FROM 
            pg_stat_user_tables
        """
    ) |> DataFrame |> sort
end

function exists(conn::LibPQ.Connection, table::String)::Bool
    # conn = dbconnect(); table = "rgsg"
    check_illegal_strings([table])
    execute(conn, "SELECT to_regclass('public.$table') IS NOT NULL AS table_exists") |> 
        DataFrame |> 
        x -> x.table_exists[1]
end

function exists(conn::LibPQ.Connection, table::String, field::String)::Bool
    # conn = dbconnect(); table = "phenotype_data"; field = "site_id"; # field = "site"
    check_illegal_strings([table])
    check_illegal_strings([field])
    execute(
        conn, 
        """
        SELECT EXISTS (
            SELECT 1 
            FROM pg_attribute 
            WHERE attrelid = 'public.$table'::regclass 
            AND attname = '$field'
            AND NOT attisdropped
        );
        """
    ) |> 
        DataFrame |> 
        x -> x.exists[1]
end

function extract_table(conn::LibPQ.Connection, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"
    !exists(conn, table) ? error("The \"$table\" does not exist!") : nothing
    execute(conn, "SELECT * FROM $table") |>
        DataFrame
end

function meta_table_name_to_field_name(x::String, to_id::Bool = false)::String
    # to_id = true
    # x = "entries"; # x = "environment_variables"; # x = "experiments"; # x = "genomes"; # x = "genotype_vcfs"; # x = "layouts"; 
    # # x = "measurements"; # x = "phenomes"; # x = "reference_genomes"; # x = "sites"; # x = "speciess"; # x = "traits"; # x = "treatments"; 
    # # x = "child"; # x = "parent"
    xs = collect(x)
    x[end] != 's' ? error("We expect a plural table name, e.g. \"entries\", \"traits\", \"sites\", or \"experiments\".") : nothing
    if x == "entries"
        to_id ? "entry_id" : "entry"
    else
        y = join(xs[1:(end-1)])
        to_id ? "$(y)_id" : y
    end
end

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
    like::Union{Nothing, String}
    in::Union{Nothing, Vector{String}, Vector{Int}, Vector{AbstractFloat}}
    between::Union{Nothing, Tuple{Int, Int}, Tuple{AbstractFloat, AbstractFloat}}
    equal_to::Union{Nothing, Int, AbstractFloat}
    less_than::Union{Nothing, Int, AbstractFloat}
    greater_than::Union{Nothing, Int, AbstractFloat}
    function Filter(
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
        # conn = dbconnect();
        # filter_like=nothing; filter_in=nothing; filter_between=nothing; filter_equal_to=nothing; filter_less_than=nothing; filter_greater_than=nothing;
        # table = "entries"; field = "name"; filter_in = String["entry_100"]; # table = "phenotype_data"; field = "entries"; filter_in = String["entry_100"]; # table = "phenotype_data"; field = "site"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "site_id"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "WQRERWE"; filter_in = String["site_1"]; # table = "phenotype_data"; field = "site"; filter_like = "site"; # table = "phenotype_data"; field = "site"; # table = "phenotype_data"; field = "entry"; filter_in = String["entry_010", "entry_020"]; # table = "phenotype_data"; field = "entry"; filter_in = String["entry_010"]; # table = "phenotype_data"; field = "entry";
        # execute(conn, "SELECT id,value FROM phenotype_data") |> DataFrame
        # table = "phenotype_data"; field = "value"; filter_in = Float64[10.515928568077884]; # table = "phenotype_data"; field = "value"; filter_between = (10, 12); # table = "phenotype_data"; field = "value"; filter_equal_to = 10.515928568077884; # table = "phenotype_data"; field = "value"; filter_less_than = 10; # table = "phenotype_data"; field = "value"; filter_greater_than = 100
        if !exists(conn, table)
            error("The \"$table\" does not exist in the database!")
        end
        sum([
            !isnothing(filter_like), 
            !isnothing(filter_in), 
            !isnothing(filter_between), 
            !isnothing(filter_equal_to), 
            !isnothing(filter_less_than), 
            !isnothing(filter_greater_than),
        ]) != 1 ? error("We expect one and only one `filter_*` argument!") : nothing
        field = if exists(conn, table, field)
            field
        else
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
        if !exists(conn, table, field)
            error("The \"$field\" field does not exist in the \"$table\" table!")
        end
        filter_in, filter_like = if isnothing(match(Regex("_id\$"), field))
            filter_in, filter_like
        else
            metatable = field == "entry_id" ? "entries" : replace(field, "_id" => "s")
            filter_in = isnothing(filter_in) ? nothing : extract_ids(conn, names=filter_in, table=metatable).id
            filter_like = isnothing(filter_like) ? nothing : extract_ids(conn, names=[filter_like], table=metatable, is_like=true).id
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
        new(table, field, filter_like, filter_in, filter_between, filter_equal_to, filter_less_than, filter_greater_than)
    end
end

function query_table(
    conn::LibPQ.Connection;
    table::String,
    output_fields::Vector{String} = ["*"],
    filters::Vector{Filter},
    verbose::Bool = false,
)::DataFrame
    # conn = dbconnect()
    # table = "phenotype_data"
    # output_fields = String["*"]
    # filters = [
    #     Filter(conn, table="phenotype_data", field="entry", filter_like="_100"),
    #     Filter(conn, table="phenotype_data", field="site", filter_in=["site_1", "site_2"]),
    #     Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)),
    # ]
    # verbose = true
    if output_fields == String["*"]
        check_illegal_strings([table])
    else
        check_illegal_strings(vcat([table], output_fields))
    end
    sql = String["SELECT $(join(output_fields, ',')) FROM $table WHERE"]
    par = String[]
    # TODO: progress meter...
    for f in filters
        # f = filters[1]
        n = length(par)
        if !isnothing(f.like)
            push!(sql, "$(f.field) LIKE $(f.like) AND")
            append!(par, String(f.like))
        elseif !isnothing(f.in)
            s = "($(join(string.("\$", (n+1):(n+length(f.in))), ',')))"
            push!(sql, "$(f.field) IN $s AND")
            append!(par, string.(f.in))
        elseif !isnothing(f.between)
            push!(sql, "$(f.field) BETWEEN \$$(n+1) AND \$$(n+2) AND")
            append!(par, string.([f.between[1],f. between[2]]))
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
    end
    sql[end] = replace(sql[end], Regex(" AND\$") => "")
    sql = join(sql, " ")
    
    df = execute(conn, sql, par) |> DataFrame

    # TODO: when we have *_id in the output fields we extract the metatable for the exact names
    for f in names(df)
        # f = names(df)[2]
        f == "id" ? continue : nothing
        isnothing(match(Regex("_id\$"), f)) ? continue : nothing

        f = replace(f, Regex("_id\$") => "")
        metatable = f == "entry" ? "entries" : "$(f)s"

        

    end



    DataFrame()
end