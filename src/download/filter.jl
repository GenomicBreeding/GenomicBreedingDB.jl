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
    Base.hash(x::Filter, h::UInt)::UInt

Compute a hash value for a `Filter` object.

This method extends Julia's hashing mechanism for `Filter` objects,
allowing them to be used in hash-based collections such as `Set` and
`Dict`. The hash is computed by sequentially combining the hash values
of all fields in the `Filter` object.

# Arguments

- `x::Filter`: Filter object to hash.
- `h::UInt`: Initial hash seed.

# Returns

- `UInt`: Hash value for the `Filter` object.

# Notes

- The hash is computed using all fields returned by `fieldnames(typeof(x))`.
- Defined for the correctness of `unique()` on a `Vector{Filter}`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> f = Filter(conn, table="entries", field="name", filter_like="_010");

julia> isa(hash(f), UInt)
true

julia> close(conn);
```
"""
function Base.hash(x::Filter, h::UInt)::UInt
    for field in fieldnames(typeof(x))
        # field = fieldnames(typeof(x))[1]
        h = hash(getfield(x, field), h)
    end
    h
end

"""
    Base.:(==)(x::Filter, y::Filter)::Bool

Determine whether two `Filter` objects are equal.

Two `Filter` objects are considered equal if all corresponding fields
have equal values. Fields are compared sequentially using `!=`, and the
comparison terminates as soon as a mismatch is found.

# Arguments

- `x::Filter`: First filter to compare.
- `y::Filter`: Second filter to compare.

# Returns

- `true` if all fields in `x` and `y` are equal.
- `false` if any field differs.

# Notes

- Equality is evaluated by comparing every field returned by
  `fieldnames(typeof(x))`.
- Unlike `Base.hash(::Filter, ::UInt)`, no fields are excluded from
  the equality comparison.
- This implementation performs a field-by-field comparison rather than
  relying on hash values.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="_01"));

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="_01"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> filters[1] == filters[2]
true

julia> filters[1] != filters[3]
true

julia> length(filters) > length(unique(filters))
true
```
"""
function Base.:(==)(x::Filter, y::Filter)::Bool
    for field in fieldnames(typeof(x))
        # field = fieldnames(typeof(x))[1]
        if getfield(x, field) != getfield(y, field)
            return false
        end
    end
    true
end
