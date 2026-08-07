"""
    define_filters(
        conn::LibPQ.Connection,
        args::AbstractDict{String};
        table::String,
    )::Tuple{Vector{Filter},Vector{ErrorException}}

Construct a collection of validated `Filter` objects from a dictionary of query
arguments.

The function translates user-supplied query arguments into `Filter` objects
whilst collecting any validation errors encountered during filter
construction. Rather than stopping at the first invalid filter, the function
attempts to construct all possible filters and returns both the successful
filters and any errors that occurred.

Arguments prefixed with `like_` are interpreted as fuzzy-search filters and
converted into `filter_like` constraints. The special field `value` is
interpreted as a numeric interval and converted into a `filter_between`
constraint. All other recognised fields are converted into `filter_in`
constraints.

For pedigree relationships stored in `entry_relationships`, user-friendly
fields such as `entry`, `child`, and `parent` are automatically mapped to the
appropriate database identifier fields (`child_id` and `parent_id`) before
filter construction.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `args::AbstractDict{String}`: Collection of query arguments.
- `table::String`: Name of the target database table.

# Returns

- `Tuple{Vector{Filter},Vector{ErrorException}}`:
    + `filters`: Successfully constructed filter objects.
    + `errors`: Errors encountered whilst constructing filters.

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
    + `entries` → `entry`
    + `traits` → `trait`
    + `species` → `species`
    + `like_entries` → `entry`
- For the `entry_relationships` table, pedigree-specific aliases are resolved
  automatically:
    + `entry` → `child_id`
    + `child` → `child_id`
    + `parent` → `parent_id`
    + `like_entry` → `child_id`
    + `like_child` → `child_id`
    + `like_parent` → `parent_id`
- This allows pedigree relationships to be filtered using human-readable entry
  names instead of internal database identifiers.
- The resulting `Filter` objects automatically resolve entry names to the
  appropriate database IDs when querying `entry_relationships`.
- This helper is intended for workflows that need to report multiple input
  errors simultaneously rather than failing on the first invalid filter.
- Returned filters can be passed directly to `query(conn, filters; ...)`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> args = Dict("like_entries" => ["_1"], "like_traits" => ["2"]);

julia> filters, errors = define_filters(conn, args, table="phenotype_data");

julia> length(filters) > 0
true

julia> length(errors) == 0
true

julia> filters, errors = define_filters(conn, args, table="entry_relationships");

julia> length(filters) > 0
true

julia> length(errors) > 0
true
```
"""
function define_filters(
    conn::LibPQ.Connection,
    args::AbstractDict{String};
    table::String,
)::Tuple{Vector{Filter},Vector{ErrorException}}
    # conn = dbconnect(); args = Dict("treatments"=>["control"], "entries" => String[], "sdgdgdfg"=> String["sdgsdg"]); table = "phenomes";
    # conn = dbconnect(); args = Dict("QIUWYGB"=>["control"], "AKJUGABIDKBIJ" => String[], "sdgdgdfg"=> String["sdgsdg"]); table = "phenomes";
    # conn = dbconnect(); args = Dict("like_entries"=>["_01"]); table = "entry_relationships";
    # conn = dbconnect(); args = Dict("like_entries"=>["_01"]); table = "entries";
    # conn = dbconnect(); args = Dict("species"=>["a"]); table = "entries";
    # conn = dbconnect(); args = Dict("like_species"=>["a"]); table = "phenomes";
    filters = Filter[]
    errors = ErrorException[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        # k = string.(keys(args))[4]; v = args[k]
        # k = string.(keys(args))[20]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field = if !isnothing(match(Regex("species"), k))
            replace(k, Regex("^like_")=>"")
        else
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        end
        try
            if is_like
                if length(v) > 1
                    error("We only accept 1 query for fuzzy search: $k = $v")
                end
                if (table == "entry_relationships") && ((field == "entry") || (field == "child"))
                    push!(filters, Filter(conn, table = table, field = "child_id", filter_like = v[1]))
                elseif (table == "entry_relationships") && (field == "parent")
                    push!(filters, Filter(conn, table = table, field = "parent_id", filter_like = v[1]))
                else
                    push!(filters, Filter(conn, table = table, field = field, filter_like = v[1]))
                end
            elseif field == "value"
                push!(filters, Filter(conn, table = table, field = field, filter_between = v))
            else
                if (table == "entry_relationships") && (field == "entry")
                    push!(filters, Filter(conn, table = table, field = "child_id", filter_in = v))
                elseif (table == "entry_relationships") && (field == "parent")
                    push!(filters, Filter(conn, table = table, field = "parent_id", filter_in = v))
                else
                    push!(filters, Filter(conn, table = table, field = field, filter_in = v))
                end
                # TODO: probably for date intervals...
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

julia> args = Dict("entries" => ["entry_001", "entry_002"], "traits" => ["trait_2", "trait_3"]);

julia> like_combinations, like_combinations_keys = combinations(args);

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

"""
    download(
        table::String;
        entries::Vector{String}=String[],
        species::Vector{String}=String[],
        entry_types::Vector{String}=String[],
        experiments::Vector{String}=String[],
        sites::Vector{String}=String[],
        treatments::Vector{String}=String[],
        measurements::Vector{String}=String[],
        traits::Vector{String}=String[],
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
        like_traits::Vector{String}=String[],
        names::Vector{String}=String[],
        notes::Vector{String}=String[],
        reference_genomes::Vector{String}=String[],
        values::Tuple{Float64,Float64}=(-Inf, +Inf),
        verbose::Bool=false,
    )::DataFrame

Download records from a database table using exact-match, fuzzy-search, and
relationship-aware filtering.

The function provides a generic database-query interface that automatically
constructs filters from the supplied arguments, applies those filters to the
requested table and any related tables, and returns the matching records as a
`DataFrame`.

For tables requiring metadata from multiple sources, the function automatically
queries and joins the necessary related tables. When multiple fuzzy-search
criteria (`like_*`) contain multiple values, all possible search-term
combinations are evaluated independently and the final result is obtained by
taking the union of all matching records.

This function is intended as a flexible schema-aware alternative to
table-specific download functions and can be used for exploratory querying
across phenotype, environmental, pedigree, phenome, genome, and metadata
tables.

# Arguments

- `table::String`: Table to query.
- `entries::Vector{String}=String[]`: Entry names to match exactly.
- `species::Vector{String}=String[]`: Species names to match exactly.
- `entry_types::Vector{String}=String[]`: Entry types to match exactly.
- `experiments::Vector{String}=String[]`: Experiment names to match exactly.
- `sites::Vector{String}=String[]`: Site names to match exactly.
- `treatments::Vector{String}=String[]`: Treatment names to match exactly.
- `measurements::Vector{String}=String[]`: Measurement names to match exactly.
- `traits::Vector{String}=String[]`: Trait names to match exactly.
- `environment_variables::Vector{String}=String[]`: Environmental-variable
  names to match exactly.
- `replications::Vector{Int64}=Int64[]`: Replication identifiers to match.
- `blocks::Vector{Int64}=Int64[]`: Block identifiers to match.
- `rows::Vector{Int64}=Int64[]`: Plot-row identifiers to match.
- `cols::Vector{Int64}=Int64[]`: Plot-column identifiers to match.
- `like_names::Vector{String}=String[]`: Dataset-name patterns for fuzzy
  matching.
- `like_notes::Vector{String}=String[]`: Note patterns for fuzzy matching.
- `like_reference_genomes::Vector{String}=String[]`: Reference-genome patterns
  for fuzzy matching.
- `like_entries::Vector{String}=String[]`: Entry-name patterns for fuzzy
  matching.
- `like_species::Vector{String}=String[]`: Species-name patterns for fuzzy
  matching.
- `like_entry_types::Vector{String}=String[]`: Entry-type patterns for fuzzy
  matching.
- `like_experiments::Vector{String}=String[]`: Experiment-name patterns for
  fuzzy matching.
- `like_sites::Vector{String}=String[]`: Site-name patterns for fuzzy matching.
- `like_treatments::Vector{String}=String[]`: Treatment-name patterns for fuzzy
  matching.
- `like_measurements::Vector{String}=String[]`: Measurement-name patterns for
  fuzzy matching.
- `like_environment_variables::Vector{String}=String[]`: Environmental-variable
  patterns for fuzzy matching.
- `like_traits::Vector{String}=String[]`: Trait-name patterns for fuzzy
  matching.
- `names::Vector{String}=String[]`: Resource names to match exactly.
- `notes::Vector{String}=String[]`: Resource notes to match exactly.
- `reference_genomes::Vector{String}=String[]`: Reference-genome names to match
  exactly.
- `values::Tuple{Float64,Float64}=(-Inf, +Inf)`: Inclusive lower and upper
  bounds for numeric-value filtering.
- `verbose::Bool=false`: If `true`, display progress information during
  filtering, querying, and joining.

# Returns

- `DataFrame`: Records matching the supplied filtering criteria.

# Throws

- Any exception raised whilst constructing filters.
- Any exception raised whilst querying the database.
- Any exception raised whilst joining related tables.

# Notes

- Filter construction is delegated to `define_filters`.
- Query execution is delegated to `query`.
- Human-readable names are automatically resolved to database identifiers
  through the `Filter` infrastructure.
- Exact-match filters are translated into SQL `IN` operations.
- Fuzzy-search filters are translated into SQL `LIKE` operations.
- Numeric intervals are translated into SQL `BETWEEN` operations.
- Multiple values supplied to one or more `like_*` arguments are treated as
  independent fuzzy-search criteria.
- When multiple `like_*` filters contain multiple values, all possible
  combinations are generated using `combinations(args)` and evaluated
  independently.
- Results from individual fuzzy-search combinations are merged using a union
  operation (`vcat`).
- Filters that are not applicable to a given table are collected as validation
  errors and ignored for that table.
- If no valid filters are generated for a table, all records from that table
  are queried.
- Depending on `table`, related tables are automatically queried and joined:
  - `phenotype_data` → `phenotype_data`, `layouts`
  - `environment_data` → `environment_data`, `layouts`
  - `entry_relationships` → `entry_relationships`, `entries`, `species`
- All other tables are queried directly without additional joins.
- Identifier and bookkeeping fields such as:
  - `id`
  - `created_at`
  - `updated_at`
  are removed from intermediate tables before joining where appropriate.
- Relationship fields are normalised automatically to facilitate joins.
- For pedigree queries, `child` is automatically renamed to `entry`.
- Joins are performed using biologically meaningful fields such as:
  - `entry`
  - `species`
  - `layout`
- Missing join values are temporarily represented as `"missing"` during join
  operations.
- The resulting DataFrame may contain columns originating from multiple related
  database tables.
- For file-based dataset tables:
    + `phenomes`
    + `genomes`
    + `genotype_vcfs`
    + `reference_genomes`
  only filters that correspond to fields represented in those tables and their
  relationships are used during query construction.
- Filters intended for observational tables (for example
  `replications`, `blocks`, `rows`, `cols`, `values`,
  `environment_variables`, and related fuzzy-search variants) are ignored when
  querying file-based dataset tables because those attributes are not stored at
  the dataset-file level.
- For `genomes`, `genotype_vcfs`, and `phenomes`, entry-based filtering is
  generally sufficient because entry names are unique across species, entry
  types, source populations, and parental combinations.
- Consequently, `species`, `entry_types`, `like_species`, and
  `like_entry_types` may not further restrict the query when entry-based
  filters are already provided.
- For `reference_genomes`, only dataset metadata filters such as:
    + `names`
    + `notes`
    + `like_names`
    + `like_notes`
  are applicable; phenotype-, environment-, layout-, and pedigree-related
  filters are ignored.
- Filters that do not correspond to fields present in the requested table or
  its supported relationships are automatically discarded during filter
  construction and do not generate query constraints.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates; import GenomicBreedingDB: download)
julia> df_phenotype_data = download("phenotype_data", like_entries=["_01", "_02"], like_traits=["_2", "_3"]);

julia> sum(.!isnothing.(match.(Regex("_01|_02"), df_phenotype_data.entry))) == nrow(df_phenotype_data)
true

julia> sum(.!isnothing.(match.(Regex("_2|_3"), df_phenotype_data.trait))) == nrow(df_phenotype_data)
true

julia> df_phenotype_data = download("phenotype_data", entries=["entry_010"], like_entries=["_01", "_02"], like_traits=["_2", "_3"]);

julia> sum(.!isnothing.(match.(Regex("_01"), df_phenotype_data.entry))) == nrow(df_phenotype_data)
true

julia> sum(.!isnothing.(match.(Regex("_2|_3"), df_phenotype_data.trait))) == nrow(df_phenotype_data)
true

julia> df_phenotype_data = download("phenotype_data", entries=["entry_100"], like_entries=["_01", "_02"], like_traits=["_2", "_3"]);

julia> nrow(df_phenotype_data) == 0
true

julia> df_environment_data = download("environment_data", like_sites=["_1", "_2"], like_treatments=["control"]);

julia> sum(.!isnothing.(match.(Regex("_1|_2"), df_environment_data.site))) == nrow(df_environment_data)
true

julia> df_entry_relationships = download("entry_relationships", like_entries=["_06", "_07"]);

julia> sum(.!isnothing.(match.(Regex("_06|_07"), df_entry_relationships.entry))) == nrow(df_entry_relationships)
true

julia> df_entries = download("entries", like_entries=["_09", "_10"], species=["a"]);

julia> nrow(df_entries) == 0
true

julia> df_entries = download("entries", like_entries=["_09", "_10"], like_species=["a"]);

julia> nrow(df_entries) > 0
true

julia> df_entries = download("entries", like_entries=["_09", "_10"], like_species=["a"], entry_types=["family"]);

julia> nrow(df_entries) > 0
true

julia> df_phenomes_1 = download("phenomes", like_entries=["_09", "_10"]);

julia> df_phenomes_2 = download("phenomes", like_entries=["_09", "_10"], species=["a"]);

julia> df_phenomes_1 == df_phenomes_2
true
```
"""
function download(
    table::String;
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    experiments::Vector{String} = String[],
    sites::Vector{String} = String[],
    treatments::Vector{String} = String[],
    measurements::Vector{String} = String[],
    traits::Vector{String} = String[],
    environment_variables::Vector{String} = String[],
    replications::Vector{Int64} = Int64[],
    blocks::Vector{Int64} = Int64[],
    rows::Vector{Int64} = Int64[],
    cols::Vector{Int64} = Int64[],
    like_names::Vector{String} = String[],
    like_notes::Vector{String} = String[],
    like_reference_genomes::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    like_experiments::Vector{String} = String[],
    like_sites::Vector{String} = String[],
    like_treatments::Vector{String} = String[],
    like_measurements::Vector{String} = String[],
    like_environment_variables::Vector{String} = String[],
    like_traits::Vector{String} = String[],
    names::Vector{String} = String[],
    notes::Vector{String} = String[],
    reference_genomes::Vector{String} = String[],
    values::Tuple{Float64,Float64} = (-Inf, +Inf),
    verbose::Bool = false,
)::DataFrame
    # display(list_all_tables(conn).table_name)
    # table = "phenotype_data"
    # table = "environment_data"
    # table = "entry_relationships"
    # table = "entries"
    # table = "phenomes"
    # table = "genomes"
    # table = "reference_genomes"
    # entries::Vector{String}=String[]
    # species::Vector{String}=String[]
    # entry_types::Vector{String}=String[]
    # experiments::Vector{String}=String[]
    # sites::Vector{String}=String[]
    # treatments::Vector{String}=String[]
    # measurements::Vector{String}=String[]
    # traits::Vector{String} = String[]
    # environment_variables::Vector{String}=String[]
    # replications::Vector{Int64}=Int64[]
    # blocks::Vector{Int64}=Int64[]
    # rows::Vector{Int64}=Int64[]
    # cols::Vector{Int64}=Int64[]
    # like_names::Vector{String}=String[]
    # like_notes::Vector{String}=String[]
    # like_reference_genomes::Vector{String}=String[]
    # like_entries::Vector{String}=String[]
    # like_species::Vector{String}=String[]
    # like_entry_types::Vector{String}=String[]
    # like_experiments::Vector{String}=String[]
    # like_sites::Vector{String}=String[]
    # like_treatments::Vector{String}=String[]
    # like_measurements::Vector{String}=String[]
    # like_environment_variables::Vector{String}=String[]
    # like_traits::Vector{String} = String[]
    # names::Vector{String}=String[]
    # notes::Vector{String}=String[]
    # reference_genomes::Vector{String}=String[]
    # values::Tuple{Float64,Float64}=(-Inf, +Inf)
    # verbose = true
    if verbose && !isnothing(match(Regex("phenomes|genomes|genotype_vcfs|reference_genomes")))
        warn(
            string(
                "Since entry names are unique across species and entry types (and source populations or parents), ",
                "then using `entries` and `like_entries` will suffice, hence `*species` and `*entry_types` parameters are ignored!",
            ),
        )
    end
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
    tables = if table == "phenotype_data"
        ["phenotype_data", "layouts"]
    elseif table == "environment_data"
        ["environment_data", "layouts"]
    elseif table == "entry_relationships"
        ["entry_relationships", "entries", "species"]
    else
        [table]
    end
    like_combinations, like_combinations_keys = combinations(args)
    df_out = nothing
    for i in eachindex(like_combinations)
        # i = 1
        for (j, key) in enumerate(like_combinations_keys)
            # j = 1; key = like_combinations_keys[j]
            args[key] = [like_combinations[i][j]]
        end
        # Query
        dfs = Dict()
        all_filters = Dict()
        all_errors = Dict()
        for t in tables
            # t = tables[1]
            filters, errors = define_filters(conn, args, table = t)
            if sum([!isnothing(match(Regex("No matches"), e.msg)) for e in errors]) > 0
                continue
            end
            df = if isempty(filters)
                ids = string.(extract_table_contents(conn, t).id)
                filters = [Filter(conn, table = t, field = "id", filter_in = ids)]
                query(conn, filters, verbose = verbose)
            else
                query(conn, filters, verbose = verbose)
            end
            all_filters[t] = filters
            all_errors[t] = errors
            dfs[t] = df
        end
        # Join
        df_out_tmp = nothing
        for t in tables
            # t = tables[1]
            # t = tables[2]
            df = try
                deepcopy(dfs[t])
            catch
                continue
            end
            try
                select!(df, Not(:id))
            catch
                nothing
            end
            try
                select!(df, Not(:created_at))
            catch
                nothing
            end
            try
                select!(df, Not(:updated_at))
            catch
                nothing
            end
            try
                select!(df, Not(:note))
            catch
                nothing
            end
            try
                rename!(df, "child" => "entry")
            catch
                nothing
            end
            df_out_tmp = if isnothing(df_out_tmp)
                df
            else
                aggregator = if t == "entries"
                    "entry"
                elseif t == "species"
                    "species"
                else
                    replace(t, Regex("s\$") => "")
                end
                try
                    rename!(df_out_tmp, "$(aggregator)_id" => aggregator)
                catch
                    nothing
                end
                try
                    rename!(df, "name" => aggregator)
                catch
                    nothing
                end
                df_out_tmp[ismissing.(df_out_tmp[:, aggregator]), aggregator] .= "missing"
                df[ismissing.(df[:, aggregator]), aggregator] .= "missing"
                innerjoin(df_out_tmp, df, on = aggregator)
            end
        end
        df_out = if isnothing(df_out)
            df_out_tmp
        else
            vcat(df_out, df_out_tmp)
        end
    end
    if isnothing(df_out)
        return DataFrame()
    end
    df_out
end
