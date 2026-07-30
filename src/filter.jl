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
The constructor validates the target table and field, verifies that the selected
filter type is compatible with the database field type, and automatically
resolves human-readable entity names into database identifiers when filtering on
foreign-key fields.

If the supplied field does not exist directly in the specified table, the
constructor attempts to infer the corresponding foreign-key field. For example,
fields such as `entry`, `entries`, `species`, `site`, or `trait` may be mapped
to their associated identifier fields (`entry_id`, `species_id`, `site_id`,
`trait_id`, etc.).

When filtering on foreign-key fields, supplied names are automatically converted
into their corresponding database identifiers using `extract_ids`. This allows
queries to be expressed using biological or experimental names rather than
internal database ids.

The constructor additionally validates that string-based filters are applied only
to string-compatible fields and that numeric filters are applied only to numeric
fields.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the target table.
- `field::String`: Name of the field on which filtering will be applied.
- `filter_like::Union{Nothing,String}=nothing`: Pattern-matching filter.
- `filter_in::Union{Nothing,Vector{String},Vector{Int},Vector{AbstractFloat}}=nothing`:
  Collection-based filter.
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
- `field::String`: Database field used for filtering.
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
- `ErrorException`: If zero or multiple filtering criteria are supplied.
- `ErrorException`: If a string filter is applied to a non-string field.
- `ErrorException`: If a numeric filter is applied to a non-numeric field.
- `ErrorException`: If no matching identifiers can be found when resolving
  foreign-key names.
- Any exception raised during schema validation or identifier resolution.

# Notes

- Exactly one filtering criterion must be supplied.
- Table validation is performed using `check(conn, table)`.
- Field validation is performed using `check(conn, table, field)`.
- If the supplied field is absent, the constructor attempts to infer a matching
  foreign-key field automatically.
- Special handling is provided for:
  - `entries` → `entry_id`
  - `species` → `species_id`
- String-based filters (`filter_like` and string-valued `filter_in`) are
  validated against the underlying database field type.
- Numeric filters (numeric-valued `filter_in`, `filter_between`, `filter_equal_to`,
  `filter_less_than`, and `filter_greater_than`) are validated against the
  underlying database field type.
- For `filter_between`, the first value must be less than or equal to the second
  value.
- Name-based filters applied to foreign-key fields are automatically translated
  into numeric ids using `extract_ids`.
- Foreign-key mappings are resolved through corresponding lookup tables:
  - `entry_id` ↔ `entries`
  - `species_id` ↔ `species`
  - `<name>_id` ↔ `<name>s`
- `filter_like` searches applied to foreign-key fields are resolved to matching
  ids and subsequently converted into an `IN` filter.
- Pattern-matching filters automatically receive `%` wildcards when not already
  supplied.
- Underscore characters are escaped to avoid unintended wildcard matching.
- The resulting `Filter` object contains fully validated and resolved values and
  is ready for use with functions such as `query`, `update_table!`, and
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

julia> x_1 = Filter(conn, table="genomes", field="entries", filter_in=["entry_001"]);

julia> x_2 = Filter(conn, table="genomes", field="entries", filter_like="%");

julia> (x_1.table == "genomes") && (x_1.field == "entry_id") && (x_1.in[1] ∈ x_2.in)
true

julia> x_1 = Filter(conn, table="phenomes", field="entries", filter_in=["entry_001"]);

julia> x_2 = Filter(conn, table="phenomes", field="entries", filter_like="%");

julia> (x_1.table == "phenomes") && (x_1.field == "entry_id") && (x_1.in[1] ∈ x_2.in)
true

julia> x_1 = Filter(conn, table="phenomes", field="traits", filter_like="_1");

julia> x_2 = Filter(conn, table="phenomes", field="traits", filter_like="trait_");

julia> (x_1.table == "phenomes") && (x_1.field == "trait_id") && (x_1.in[1] ∈ x_2.in)
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
        # table = "phenomes"; field = "treatment"; filter_in = ["control"]
        check(conn, table) # checks for illegal strings
        sum([
            !isnothing(filter_like),
            !isnothing(filter_in),
            !isnothing(filter_between),
            !isnothing(filter_equal_to),
            !isnothing(filter_less_than),
            !isnothing(filter_greater_than),
        ]) != 1 ? error("We expect one and only one `filter_*` argument!") : nothing
        field = try
            check(conn, table, field) # checks for illegal strings
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
        filter_in, filter_like = if isnothing(match(Regex("_id\$"), field))
            # No need to look at a meta table or relationship table
            !isnothing(filter_like) ? check(conn, table, field, String) : nothing
            if !isnothing(filter_in)
                try
                    check(conn, table, field, String)
                catch
                    check(conn, table, field, Float64)
                end
            end
            filter_in, filter_like
        else
            # Will need to look at a meta table and if necessary (genomes and phenomes tables), relationship table
            metatable = if field == "entry_id"
                "entries"
            elseif field == "species_id"
                "species"
            else
                replace(field, "_id" => "s")
            end
            reltable, metatable = if (table == "genomes") || (table == "phenomes")
                reltable = string(table, "_", metatable)
                check(conn, reltable, field)
                check(conn, metatable, "id")
                reltable, metatable
            else
                reltable = nothing
                check(conn, table, field)
                check(conn, metatable, "id")
                reltable, metatable
            end
            dict_filters_in_and_like::Dict{String,Union{Nothing,String,Vector{String}}} =
                Dict("filter_in" => filter_in, "filter_like" => filter_like)
            for (k, v) in dict_filters_in_and_like
                # k = string.(keys(dict_filters_in_and_like))[1]; v = dict_filters_in_and_like[k]
                isnothing(v) ? continue : nothing
                v, is_like = if k == "filter_like"
                    [v], true
                else
                    v, false
                end
                tmp = if isnothing(reltable)
                    # `table` is connected directly to `metatable`
                    extract_ids(conn, names = v, table = metatable, is_like = is_like).id
                else
                    # `table` is connected to `metatable` via a `reltable`
                    relfield = if table == "genomes"
                        "genome_id"
                    elseif table == "phenomes"
                        "phenome_id"
                    else
                        error("Unexpected table=\"$table\" connected to metable=\"$metatable\" via reltable=\"$reltable\"!")
                    end
                    check_illegal_strings([relfield, reltable, field])
                    ids_meta = extract_ids(conn, names = v, table = metatable, is_like = is_like).id
                    ids_rel = String[]
                    for id in ids_meta
                        # id = ids_meta[1]
                        df_tmp = execute(conn, "SELECT $relfield FROM $reltable WHERE $field = \$1", [id]) |> DataFrame
                        isempty(df_tmp) ? continue : nothing
                        push!(ids_rel, df_tmp[1, 1])
                    end
                    ids_rel
                end
                unique!(tmp)
                if length(tmp) == 0
                    error("No matches for \"$(join(v, "\", \""))\" in \"$metatable\" table!")
                end
                dict_filters_in_and_like[k] = tmp
            end
            # Here we set the `filter_like` into `filter_in` because 
            # we already assigned the query matches from above and no longer need to do fuzzy search
            if !isnothing(dict_filters_in_and_like["filter_like"])
                dict_filters_in_and_like["filter_in"] = deepcopy(dict_filters_in_and_like["filter_like"])
                dict_filters_in_and_like["filter_like"] = nothing
            end
            (dict_filters_in_and_like["filter_in"], dict_filters_in_and_like["filter_like"])
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
        if !isnothing(filter_between) && (filter_between[1] > filter_between[2])
            error(
                string(
                    "The first value (",
                    filter_between[1],
                    ") in the `filter_between` tuple should not be greater than the second value (",
                    filter_between[2],
                    ")!",
                ),
            )
        end
        # Numeric type checks
        !isnothing(filter_between) ? check(conn, table, field, Float64) : nothing
        !isnothing(filter_equal_to) ? check(conn, table, field, Float64) : nothing
        !isnothing(filter_less_than) ? check(conn, table, field, Float64) : nothing
        !isnothing(filter_greater_than) ? check(conn, table, field, Float64) : nothing
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

Convert a collection of validated `Filter` objects into parameterised SQL filter
clauses and their associated query parameters.

The function translates `Filter` instances into SQL fragments suitable for
inclusion in a `WHERE` clause. Because `Filter` objects perform schema
validation, field validation, type checking, identifier resolution, and input
sanitisation during construction, this function can safely focus on generating
parameterised SQL expressions and their corresponding query parameters.

The resulting SQL fragments and parameter vector can be incorporated directly
into parameterised SQL queries, update statements, and delete operations.

Special handling is provided for PostgreSQL enum fields such as
`entry_type` and `relationship_type`, which are explicitly cast to text before
applying string-based filtering operations.

# Arguments

- `filters::Vector{Filter}`: Collection of validated filters to convert into SQL
  clauses.
- `verbose::Bool=false`: If `true`, display progress information whilst
  processing filters.

# Returns

- `Tuple{Vector{String},Vector{String}}`:
  - `sql`: Vector of SQL filter expressions suitable for appending to a
    `WHERE` clause.
  - `par`: Vector of parameter values corresponding to the generated SQL
    placeholders.

# Throws

- `ErrorException`: If a filter does not contain a valid filtering criterion.

# Notes

- `Filter` objects are expected to have already been validated before reaching
  this function.
- Table names, field names, filter types, and filter values have already been
  checked during `Filter` construction.
- Foreign-key name resolution and identifier lookup have already been performed
  by `Filter` where applicable.
- SQL fragments are generated using parameter placeholders (`\$1`, `\$2`, ...)
  rather than embedding values directly into query strings.
- This separation between SQL text and parameter values helps prevent SQL
  injection attacks.
- The function supports the following filter types:
  - `like` → `ILIKE`
  - `in` → `IN (...)`
  - `between` → `BETWEEN ... AND ...`
  - `equal_to` → `=`
  - `less_than` → `<`
  - `greater_than` → `>`
- Parameter numbering is generated dynamically based on the number of
  previously accumulated parameters.
- All parameter values are converted to strings before being returned.
- The PostgreSQL enum fields `entry_type` and `relationship_type` are cast to
  text when using `ILIKE` or `IN` filters.
- All generated clauses begin with `AND`, allowing them to be concatenated
  directly after a base condition such as `WHERE 1=1`.
- Filters using `IN` generate one SQL placeholder per supplied value.
- Progress reporting is available when `verbose=true`.
- The generated SQL fragments are intended to be incorporated into larger SQL
  statements and are not executed directly.
- This function is used internally by higher-level database operations such as
  `query`, `update_table!`, and `delete_names!`.

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

julia> table = "entries";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="name", filter_like="10"));

julia> push!(filters, Filter(conn, table=table, field="entry_type", filter_in=["family"]));

julia> filters_cat, par = concat_filters(filters);

julia> (length(filters_cat) == 2) && (length(par) == 2)
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
            if (f.field == "entry_type") || (f.field == "relationship_type")
                push!(sql, "AND $(f.field)::text ILIKE \$$(n+1)")
            else
                push!(sql, "AND $(f.field) ILIKE \$$(n+1)")
            end
            append!(par, [String(f.like)])
        elseif !isnothing(f.in)
            s = "($(join(string.("\$", (n+1):(n+length(f.in))), ',')))"
            if (f.field == "entry_type") || (f.field == "relationship_type")
                push!(sql, "AND $(f.field)::text IN $s") # why not just use ANY? Because we have potentially more than one filter and LibPQ does not seem to allow me to use parameters with individual elements and vectors, hence multiple parameters and LibPQ does not seem
            else
                push!(sql, "AND $(f.field) IN $s") # why not just use ANY? Because we have potentially more than one filter and LibPQ does not seem to allow me to use parameters with individual elements and vectors, hence multiple parameters and LibPQ does not seem
            end
            append!(par, string.(f.in))
        elseif !isnothing(f.between)
            push!(sql, "AND $(f.field) BETWEEN \$$(n+1) AND \$$(n+2)")
            append!(par, string.([f.between[1], f.between[2]]))
        elseif !isnothing(f.equal_to)
            push!(sql, "AND $(f.field) = \$$(n+1)")
            append!(par, string.([f.equal_to]))
        elseif !isnothing(f.less_than)
            push!(sql, "AND $(f.field) < \$$(n+1)")
            append!(par, string.([f.less_than]))
        elseif !isnothing(f.greater_than)
            push!(sql, "AND $(f.field) > \$$(n+1)")
            append!(par, string.([f.greater_than]))
        else
            error("No filtering defined in $f.")
        end
        verbose ? ProgressMeter.next!(pb) : nothing
    end
    verbose ? ProgressMeter.finish!(pb) : nothing
    return (sql, par)
end
