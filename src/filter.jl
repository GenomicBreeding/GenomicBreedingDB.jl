"""
    Filter(
        conn::LibPQ.Connection;
        table::String,
        field::String,
        filter_like::Union{Nothing,String}=nothing,
        filter_in::Union{
            Nothing,
            Vector{String},
            Vector{Int},
            Vector{AbstractFloat}
        }=nothing,
        filter_between::Union{
            Nothing,
            Tuple{Int,Int},
            Tuple{AbstractFloat,AbstractFloat}
        }=nothing,
        filter_equal_to::Union{Nothing,Int,AbstractFloat}=nothing,
        filter_less_than::Union{Nothing,Int,AbstractFloat}=nothing,
        filter_greater_than::Union{Nothing,Int,AbstractFloat}=nothing,
    )

Construct a validated database filter for querying, updating, or deleting
records.

A `Filter` encapsulates a single filtering criterion applied to a database table.
The constructor validates the target table and field, ensures that exactly one
filter condition has been supplied, and automatically resolves human-readable
entity names into database identifiers when filtering on foreign-key fields.

If a supplied field does not exist in the target table, the constructor attempts
to infer a related foreign-key field. For example, fields such as `entry`,
`entries`, `species`, `site`, or `trait` may be mapped automatically to their
corresponding identifier fields (`entry_id`, `species_id`, `site_id`,
`trait_id`, etc.) when those fields exist in the database schema.

When filtering on identifier fields, supplied names are automatically translated
into their corresponding database ids using `extract_ids`. This allows filters to
be expressed using meaningful biological or experimental identifiers rather than
internal database keys.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table to filter.
- `field::String`: Name of the field on which filtering will be applied.
- `filter_like::Union{Nothing,String}=nothing`: Pattern-matching filter using SQL
  `ILIKE` semantics.
- `filter_in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}=nothing`:
  Filter matching any value in the supplied collection.
- `filter_between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}}=nothing`:
  Inclusive range filter.
- `filter_equal_to::Union{Nothing,Int,AbstractFloat}=nothing`: Exact numeric
  equality filter.
- `filter_less_than::Union{Nothing,Int,AbstractFloat}=nothing`: Numeric
  less-than filter.
- `filter_greater_than::Union{Nothing,Int,AbstractFloat}=nothing`: Numeric
  greater-than filter.

# Fields

- `table::String`: Target database table.
- `field::String`: Database field used in the filter expression.
- `like::Union{Nothing,String}`: Pattern-matching filter value.
- `in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}`:
  Collection-based filter values.
- `between::Union{Nothing,Tuple{Int,Int},Tuple{AbstractFloat,AbstractFloat}}`:
  Inclusive range filter values.
- `equal_to::Union{Nothing,Int,AbstractFloat}`: Equality filter value.
- `less_than::Union{Nothing,Int,AbstractFloat}`: Less-than filter value.
- `greater_than::Union{Nothing,Int,AbstractFloat}`: Greater-than filter value.

# Throws

- `ErrorException`: If the target table does not exist.
- `ErrorException`: If the specified field cannot be resolved to a valid field.
- `ErrorException`: If zero or multiple filter criteria are supplied.
- `ErrorException`: If supplied names cannot be resolved to database ids.
- `ErrorException`: If no database records match a relational filter value.
- Any exception raised during database validation or identifier resolution.

# Notes

- Exactly one filtering criterion must be supplied.
- Connection and schema validation are performed using `check`.
- If the supplied field is not present in the table, the constructor attempts to
  infer a suitable foreign-key field by appending `_id`.
- Special handling is provided for:
  - `entries` → `entry_id`
  - `species` → `species_id`
- Name-based filters applied to foreign-key fields are automatically translated
  into numeric identifiers using `extract_ids`.
- Foreign-key mappings are resolved through the corresponding lookup tables:
  - `entry_id` ↔ `entries`
  - `species_id` ↔ `species`
  - `<name>_id` ↔ `<name>s`
- `filter_like` searches on foreign-key fields are resolved to matching ids and
  subsequently converted into `IN` filters.
- Pattern-matching filters automatically receive surrounding `%` wildcards when
  not already supplied.
- Underscore characters are escaped to prevent unintended SQL wildcard matching.
- The resulting `Filter` object contains fully resolved values and is ready for
  use by functions such as `query_table`, `concat_filters`, `update_table!`, and
  `delete_names!`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try Filter(conn, table="entries", field="name", filter_like="_010", filter_in="entry_100"); catch; false; end
false

julia> Filter(conn, table="entries", field="name", filter_like="010")
Filter("entries", "name", "%010%", nothing, nothing, nothing, nothing, nothing)

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

julia> x = Filter(conn, table="entries", field="name", filter_like="entry_100");

julia> !isnothing(match(Regex("^%"), x.like))
true

julia> !isnothing(match(Regex("_"), x.like))
true

julia> close(conn);
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
            elseif field == "species"
                "species_id"
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
            metatable = if field == "entry_id"
                "entries"
            elseif field == "species_id"
                "species"
            else
                replace(field, "_id" => "s")
            end
            filter_in = if isnothing(filter_in)
                nothing
            else
                tmp = extract_ids(conn, names = filter_in, table = metatable).id
                if length(tmp) == 0
                    error("No matches for \"$(join(filter_in, "\", \""))\" in \"$metatable\" table!")
                end
                tmp
            end
            filter_like = if isnothing(filter_like)
                nothing
            else
                tmp = extract_ids(conn, names = [filter_like], table = metatable, is_like = true).id
                if length(tmp) == 0
                    error("No matches for \"%$filter_like%\" in \"$metatable\" table!")
                end
                tmp
            end

            if !isnothing(filter_like)
                # Here we set the `filter_like` into `filter_in` because we already assigned the query matches from above an no longer need to do fuzzy search
                (filter_like, nothing)
            else
                (filter_in, nothing)
            end
        end
        filter_like = if !isnothing(filter_like)
            filter_like = if isnothing(match(Regex("%"), filter_like))
                "%$(filter_like)%"
            else
                filter_like
            end
            replace(filter_like, "_" => "\\_")
        else
            filter_like
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
    Base.hash(
        x::Filter,
        h::UInt,
    )::UInt

Compute a hash value for a `Filter` object.

The method extends Julia's hashing interface for the `Filter` type by combining
the hash values of all fields contained within the object. The resulting hash
depends on the values of every field, ensuring that filters with identical
contents produce identical hash values.

This implementation enables `Filter` objects to be used reliably in hashed data
structures such as `Dict`, `Set`, and other collections that depend on hash-based
lookup.

# Arguments

- `x::Filter`: Filter object to hash.
- `h::UInt`: Initial hash seed.

# Returns

- `UInt`: Hash value representing the contents of the `Filter` object.

# Notes

- Hash values are generated by iteratively combining the hashes of all fields in
  the `Filter` object.
- Field values are processed in the order returned by
  `fieldnames(typeof(x))`.
- The implementation is compatible with Julia's standard hashing framework.
- Objects containing identical field values will produce identical hash values
  when supplied with the same hash seed.
- This method should remain consistent with any corresponding `isequal`
  implementation for the `Filter` type.

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
    Base.:(==)(
        x::Filter,
        y::Filter,
    )::Bool

Determine whether two `Filter` objects are equal.

Two `Filter` objects are considered equal when all corresponding fields contain
identical values. Equality is evaluated by comparing each field in the order
defined by the `Filter` structure, and comparison terminates immediately when a
difference is detected.

This method extends Julia's standard equality operator for the `Filter` type and
provides behaviour consistent with the custom `hash` implementation. The primary
use of this method is so that the `unique` function on a vector of `Filter` objects
work as intended.

# Arguments

- `x::Filter`: First filter object.
- `y::Filter`: Second filter object.

# Returns

- `Bool`: `true` if all fields of `x` and `y` are equal; otherwise `false`.

# Notes

- Equality is determined by comparing every field in the `Filter` structure.
- Comparison is performed using the `!=` operator on each corresponding field.
- Evaluation stops as soon as a mismatch is detected.
- Filters that contain identical values in all fields are considered equal.
- This implementation is intended to remain consistent with
  `Base.hash(::Filter, ::UInt)`, enabling reliable use of `Filter` objects in
  hash-based collections such as `Set` and `Dict`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> filters[1] == filters[2]
true

julia> filters[1] != filters[3]
true

julia> length(filters) > length(unique(filters))
true

julia> close(conn);
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


"""
    concat_filters(
        filters::Vector{Filter};
        verbose::Bool=false,
    )::Tuple{Vector{String},Vector{String}}

Convert a collection of `Filter` objects into SQL filter clauses and their
associated query parameters.

The function translates validated `Filter` instances into parameterised SQL
fragments suitable for inclusion in a `WHERE` clause. SQL expressions and query
parameters are accumulated in the order supplied, ensuring that parameter indices
remain aligned with the generated placeholders.

The resulting SQL fragments and parameter vector can be used directly in
subsequent database queries or update statements.

# Arguments

- `filters::Vector{Filter}`: Collection of filters to convert into SQL clauses.
- `verbose::Bool=false`: If `true`, display progress information whilst
  processing the filters.

# Returns

- `Tuple{Vector{String},Vector{String}}`:
  - `sql`: Vector of SQL filter expressions.
  - `par`: Vector of parameter values corresponding to the generated SQL
    placeholders.

# Throws

- `ErrorException`: If a filter does not contain a valid filtering criterion.

# Notes

- SQL fragments are generated using parameter placeholders rather than embedding
  values directly into query strings.
- The function supports the following filter types:
  - `like` → `ILIKE`
  - `in` → `IN (...)`
  - `between` → `BETWEEN ... AND ...`
  - `equal_to` → equality comparison
  - `less_than` → less-than comparison
  - `greater_than` → greater-than comparison
- Parameter numbering is generated dynamically based on the number of previously
  accumulated parameters.
- All parameter values are converted to strings before being returned.
- Progress reporting is available when `verbose=true`.
- The generated SQL fragments are intended to be concatenated with an existing
  query rather than executed directly.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 20)));

julia> filters_cat, par = concat_filters(filters);

julia> (length(filters_cat) == 2) && (length(par) == 4)
true

julia> close(conn);
```
"""
function concat_filters(filters::Vector{Filter}; verbose::Bool = false)::Tuple{Vector{String},Vector{String}}
    sql = String[]
    par = String[]
    pb = ProgressMeter.Progress(length(filters), desc = "Concatenating the filters...")
    for f in filters
        # f = filters[1]
        n = length(par)
        if !isnothing(f.like)
            push!(sql, "AND $(f.field) ILIKE \$$(n+1)")
            append!(par, [String(f.like)])
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
    verbose ? ProgressMeter.finish!(pb) : nothing
    return (sql, par)
end
