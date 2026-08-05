"""
    define_filters(
        conn::LibPQ.Connection,
        args::AbstractDict{String};
        table::String,
    )::Tuple{Vector{Filter},Vector{ErrorException}}

Construct a collection of validated `Filter` objects from a dictionary of query
arguments.

The function translates user-supplied query arguments into `Filter` objects
whilst collecting any validation errors that occur during filter
construction. Unlike higher-level query helpers that immediately throw an
exception when an invalid filter is encountered, this function attempts to
construct all possible filters and records any resulting errors.

Arguments prefixed with `like_` are interpreted as fuzzy-search filters and
converted into `filter_like` constraints. The special field `value` is
interpreted as a numeric interval and converted into a `filter_between`
constraint. All other recognised fields are converted into `filter_in`
constraints.

This helper is useful when validating user input because valid and invalid
filters can be inspected separately.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `args::AbstractDict{String}`: Collection of query arguments.
- `table::String`: Name of the target database table.

# Returns

- `Tuple{Vector{Filter},Vector{ErrorException}}`:
  - `filters`: Successfully constructed filter objects.
  - `errors`: Errors encountered whilst constructing filters.

# Throws

- This function does not rethrow filter-construction errors.
- Any unexpected exception occurring outside filter construction may still be
  propagated.

# Notes

- Empty filter values are ignored.
- Arguments prefixed with `like_` are converted into `filter_like`
  constraints.
- Fuzzy-search filters accept only a single search term.
- The special field `value` is converted into a `filter_between`
  constraint.
- All other recognised fields are converted into `filter_in` constraints.
- Filter construction and validation are delegated to the `Filter`
  constructor.
- Invalid filters do not prevent valid filters from being constructed.
- Errors are collected and returned rather than immediately thrown.
- Duplicate filters are removed before returning.
- Duplicate errors are removed before returning.
- Field names are normalised before filter construction:
  - `entries` → `entry`
  - `traits` → `trait`
  - `species` → `species`
  - `like_entries` → `entry`
- This helper is intended for workflows that need to report multiple input
  errors simultaneously rather than failing on the first invalid filter.
- Returned filters can be passed directly to `query(conn, filters; ...)`.

# Examples

"""
function define_filters(
    conn::LibPQ.Connection,
    args::AbstractDict{String};
    table::String,
)::Tuple{Vector{Filter}, Vector{ErrorException}}
    # conn = dbconnect(); args = Dict("treatments"=>["control"], "entries" => String[], "sdgdgdfg"=> String["sdgsdg"]); table = "phenomes";
    # conn = dbconnect(); args = Dict("QIUWYGB"=>["control"], "AKJUGABIDKBIJ" => String[], "sdgdgdfg"=> String["sdgsdg"]); table = "phenomes";
    filters = Filter[]
    errors = ErrorException[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        # k = string.(keys(args))[2]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field = replace(k, Regex("ies\$")=>"y") |> 
            x -> replace(x, Regex("s\$")=>"") |> 
            x -> replace(x, Regex("^like_")=>"")
        try
            if is_like
                if length(v) > 1
                    error("We only accept 1 query for fuzzy search: $k = $v")
                end
                push!(filters, Filter(conn, table = table, field = field, filter_like = v[1]))
            elseif field == "value"
                push!(filters, Filter(conn, table = table, field = field, filter_between = v))
            else
                # TODO: probably for date intervals...
                push!(filters, Filter(conn, table = table, field = field, filter_in = v))
            end
        catch e
            push!(errors, e)
        end
    end
    unique!(filters)
    unique!(errors)
    filters, errors
end

"""
    combinations(
        args::AbstractDict{String},
    )::Tuple{Vector{Union{Nothing,Vector{String}}},Vector{String}}

Generate all combinations of fuzzy-search (`like_*`) query terms.

The function extracts all non-empty `like_*` arguments from a collection of
query arguments and constructs the Cartesian product of their values. The
resulting combinations are intended for workflows in which each fuzzy-search
combination must be evaluated independently and the final result obtained by
taking the union of all matches.

This approach is necessary because individual `filter_like` filters are treated
as independent fuzzy-search queries that are subsequently combined after query
execution.

# Arguments

- `args::AbstractDict{String}`: Collection of query arguments potentially
  containing one or more `like_*` fields.

# Returns

- `Tuple{Vector{Union{Nothing,Vector{String}}},Vector{String}}`:
  - `like_combinations`: All fuzzy-search combinations derived from the
    supplied `like_*` arguments. Returns `[nothing]` when no fuzzy-search
    arguments are provided.
  - `like_combinations_keys`: Names of the `like_*` fields corresponding to
    each position within the generated combinations.

# Throws

- Any exception raised whilst constructing Cartesian products of supplied
  fuzzy-search values.

# Notes

- Only arguments whose names begin with `like_` are considered.
- Empty `like_*` arguments are ignored.
- All possible combinations of supplied fuzzy-search values are generated using
  a Cartesian-product approach.
- The ordering of values within each combination corresponds to the ordering of
  fields stored in `like_combinations_keys`.
- If a single `like_*` argument is supplied, one combination is generated for
  each provided search term.
- If multiple `like_*` arguments are supplied, every possible combination of
  search terms is generated.
- When no non-empty `like_*` arguments are supplied,
  `like_combinations == [nothing]`.
- The function is primarily intended for download workflows that perform
  multiple `ILIKE` queries and subsequently combine matching records using a
  set-union operation.
- This helper is used to avoid constructing complex multi-pattern fuzzy-search
  filters whilst preserving predictable query behaviour.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> args = Dict("like_entries" => ["_1", "_2"], "like_traits" => ["2", "3"]);

julia> like_combinations, like_combinations_keys = combinations(args);

julia> length(like_combinations) == 4
true

julia> length(like_combinations[1]) == 2
true

julia> length(like_combinations_keys) == 2
true
```
"""
function combinations(args::AbstractDict{String})::Tuple{Vector{Union{Nothing,Vector{String}}},Vector{String}}
    # Extract all possible combinations of the ILIKE queries because,
    # we will perform a union on all the matches and hence will have to loop over Filters.
    # args = Dict("like_entries" => ["_1", "_2"], "like_traits" => ["2", "3"])
    # args = Dict("like_entries" => [], "like_traits" => [])
    # args = Dict("like_ASDFGHJKL" => [], "like_traits" => [])
    like_combinations::Vector{Union{Nothing,Vector{String}}} = []
    like_combinations_keys::Vector{String} = []
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        isnothing(match(Regex("^like_"), k)) ? continue : nothing
        isempty(v) ? continue : nothing
        like_combinations = if isempty(like_combinations)
            [[vi] for vi in v]
        else
            X = collect(Base.Iterators.product(like_combinations, v))
            x = collect.(reshape(X, prod(size(X))))
            if isa(like_combinations[1], Vector)
                y = []
                for i in eachindex(x)
                    # i = 1
                    push!(y, vcat(x[i][1]..., x[i][2]))
                end
                y
            else
                x
            end
        end
        push!(like_combinations_keys, k)
    end
    like_combinations = if isempty(like_combinations)
        [nothing]
    else
        like_combinations
    end
    like_combinations, like_combinations_keys
end

function download(
    table::String;
    entries::Vector{String}=String[],
    species::Vector{String}=String[],
    entry_types::Vector{String}=String[],
    experiments::Vector{String}=String[],
    sites::Vector{String}=String[],
    treatments::Vector{String}=String[],
    measurements::Vector{String}=String[],
    traits::Vector{String} = String[],
    environment_variables::Vector{String}=String[],
    replications::Vector{Int64}=Int64[],
    blocks::Vector{Int64}=Int64[],
    rows::Vector{Int64}=Int64[],
    cols::Vector{Int64}=Int64[],
    like_names::Vector{String}=String[],
    like_notes::Vector{String}=String[],
    like_reference_genomes::Vector{String}=String[],
    like_entries::Vector{String}=String[],
    like_species::Vector{String}=String[],
    like_entry_types::Vector{String}=String[],
    like_experiments::Vector{String}=String[],
    like_sites::Vector{String}=String[],
    like_treatments::Vector{String}=String[],
    like_measurements::Vector{String}=String[],
    like_environment_variables::Vector{String}=String[],
    like_traits::Vector{String} = String[],
    names::Vector{String}=String[],
    notes::Vector{String}=String[],
    reference_genomes::Vector{String}=String[],
    values::Tuple{Float64,Float64}=(-Inf, +Inf),
    keep_id_and_do_not_unstack::Bool=false,
    verbose::Bool=false,
)::DataFrame
    # table = "phenotype_data"
    # table = "phenomes"
    # table = "environment_data"
    # table = "entry_relationships"
    entries::Vector{String}=String[]
    species::Vector{String}=String[]
    entry_types::Vector{String}=String[]
    experiments::Vector{String}=String[]
    sites::Vector{String}=String[]
    treatments::Vector{String}=String[]
    measurements::Vector{String}=String[]
    traits::Vector{String} = String[]
    environment_variables::Vector{String}=String[]
    replications::Vector{Int64}=Int64[]
    blocks::Vector{Int64}=Int64[]
    rows::Vector{Int64}=Int64[1,5]
    cols::Vector{Int64}=Int64[]
    like_names::Vector{String}=String[]
    like_notes::Vector{String}=String[]
    like_reference_genomes::Vector{String}=String[]
    like_entries::Vector{String}=String["_01"]
    like_species::Vector{String}=String[]
    like_entry_types::Vector{String}=String[]
    like_experiments::Vector{String}=String[]
    like_sites::Vector{String}=String[]
    like_treatments::Vector{String}=String["con"]
    like_measurements::Vector{String}=String[]
    like_environment_variables::Vector{String}=String[]
    like_traits::Vector{String} = String[]
    names::Vector{String}=String[]
    notes::Vector{String}=String[]
    reference_genomes::Vector{String}=String[]
    values::Tuple{Float64,Float64}=(-Inf, +Inf)
    verbose = true

    conn = dbconnect()
    args = Dict(
        "entries" => entries,
        "species" => species,
        "entry_types" => entry_types,
        "experiments" => experiments,
        "sites" => sites,
        "treatments" => treatments,
        "measurements" => measurements,
        "traits" => traits,
        "environment_variables" => environment_variables,
        "replications" => replications,
        "blocks" => blocks,
        "rows" => rows,
        "cols" => cols,
        "like_names" => like_names,
        "like_notes" => like_notes,
        "like_reference_genomes" => like_reference_genomes,
        "like_entries" => like_entries,
        "like_species" => like_species,
        "like_entry_types" => like_entry_types,
        "like_experiments" => like_experiments,
        "like_sites" => like_sites,
        "like_treatments" => like_treatments,
        "like_measurements" => like_measurements,
        "like_environment_variables" => like_environment_variables,
        "like_traits" => like_traits,
        "names" => names,
        "notes" => notes,
        "reference_genomes" => reference_genomes,
        "values" => values,
    )

    all_tables = list_all_tables(conn).table_name
    assign_tables = Dict(
        "phenotype_data" => ["phenotype_data", "layouts"],
        "phenomes" => ["phenomes"],
        "environment_data" => ["environment_data", "layouts"],
        "entry_relationships" => ["entry_relationships", "entries", "species"],
    )
    tables = assign_tables[table]
   

    dfs = Dict()
    all_filters = Dict()
    all_errors = Dict()
    for t in  tables
        # t = tables[1]
        filters, errors = define_filters(conn, args, table=t)
        df = if isempty(filters)
            ids = string.(extract_table_contents(conn, t).id)
            filters = [Filter(conn, table=t, field="id", filter_in=ids)]
            query(conn, filters, verbose=verbose)
        else
            query(conn, filters, verbose=verbose)
        end
        all_filters[t] = filters
        all_errors[t] = errors
        dfs[t] = df
    end

    df_out = nothing
    for t in  tables
        # t = tables[1]
        # t = tables[2]
        df = deepcopy(dfs[t])
        try select!(df, Not(:id)); catch; nothing; end
        try select!(df, Not(:created_at)); catch; nothing; end
        try select!(df, Not(:updated_at)); catch; nothing; end
        try select!(df, Not(:note)); catch; nothing; end
        try rename!(df, "child" => "entry"); catch; nothing; end
        df_out = if isnothing(df_out)
            df
        else
            aggregator = if t == "entries"
                "entry"
            elseif t == "species"
                "species"
            else
                replace(t, Regex("s\$") => "")
            end
            try rename!(df_out, "$(aggregator)_id" => aggregator); catch; nothing; end
            try rename!(df, "name" => aggregator); catch; nothing; end
            df_out[ismissing.(df_out[:, aggregator]), aggregator] .= "missing"
            df[ismissing.(df[:, aggregator]), aggregator] .= "missing"
            innerjoin(df_out, df, on=aggregator)
        end
    end
    df_out

end