"""
    download_phenotype_data(;
        experiments::Vector{String}=String[],
        sites::Vector{String}=String[],
        treatments::Vector{String}=String[],
        measurements::Vector{String}=String[],
        entries::Vector{String}=String[],
        species::Vector{String}=String[],
        entry_types::Vector{String}=String[],
        traits::Vector{String}=String[],
        like_experiments::Vector{String}=String[],
        like_sites::Vector{String}=String[],
        like_treatments::Vector{String}=String[],
        like_measurements::Vector{String}=String[],
        like_entries::Vector{String}=String[],
        like_species::Vector{String}=String[],
        like_entry_types::Vector{String}=String[],
        like_traits::Vector{String}=String[],
        replications::Vector{Int64}=Int64[],
        blocks::Vector{Int64}=Int64[],
        rows::Vector{Int64}=Int64[],
        cols::Vector{Int64}=Int64[],
        keep_id_and_do_not_unstack::Bool=false,
        verbose::Bool=false,
    )::DataFrame

Download phenotype data from the database using a flexible collection of exact
and partial-match filters.

The function retrieves data from the `phenotype_data` table and automatically
applies additional filtering using metadata stored in the related `entries` and
`layouts` tables. Filters may be specified using exact matches (`entries`,
`sites`, `traits`, etc.) or pattern-matching searches (`like_entries`,
`like_sites`, `like_traits`, etc.).

Data retrieval is performed in three stages. The function first queries the
`phenotype_data` table, then augments and filters the result using the
associated `entries` table, and finally incorporates information from the
`layouts` table. The resulting dataset is optionally reshaped into a wide-format
table suitable for downstream analysis.

By default, phenotype measurements are unstacked such that trait names become
columns and each row corresponds to a unique observational unit.

# Arguments

- `experiments::Vector{String}=String[]`: Experiment names to match exactly.
- `sites::Vector{String}=String[]`: Site names to match exactly.
- `treatments::Vector{String}=String[]`: Treatment names to match exactly.
- `measurements::Vector{String}=String[]`: Measurement names to match exactly.
- `entries::Vector{String}=String[]`: Entry names to match exactly.
- `species::Vector{String}=String[]`: Species names to match exactly.
- `entry_types::Vector{String}=String[]`: Entry types to match exactly.
- `traits::Vector{String}=String[]`: Trait names to match exactly.
- `like_experiments::Vector{String}=String[]`: Experiment patterns for
  partial matching.
- `like_sites::Vector{String}=String[]`: Site patterns for partial matching.
- `like_treatments::Vector{String}=String[]`: Treatment patterns for partial
  matching.
- `like_measurements::Vector{String}=String[]`: Measurement patterns for
  partial matching.
- `like_entries::Vector{String}=String[]`: Entry patterns for partial matching.
- `like_species::Vector{String}=String[]`: Species patterns for partial
  matching.
- `like_entry_types::Vector{String}=String[]`: Entry-type patterns for partial
  matching.
- `like_traits::Vector{String}=String[]`: Trait patterns for partial matching.
- `replications::Vector{Int64}=Int64[]`: Replication identifiers to match.
- `blocks::Vector{Int64}=Int64[]`: Block identifiers to match.
- `rows::Vector{Int64}=Int64[]`: Plot-row identifiers to match.
- `cols::Vector{Int64}=Int64[]`: Plot-column identifiers to match.
- `keep_id_and_do_not_unstack::Bool=false`: If `true`, return the long-format
  table without reshaping.
- `verbose::Bool=false`: If `true`, display progress messages throughout the
  download and processing workflow.

# Returns

- `DataFrame`: Phenotype data matching the supplied filters.

# Throws

- `ErrorException`: If one or more filters fail validation.
- `ErrorException`: If a referenced table, field, or filter value is invalid.
- Any database exception raised during querying.
- Any exception raised whilst reshaping or joining intermediate tables.

# Notes

- A database connection is opened automatically and closed before returning.
- Exact-match filters are translated into `IN` clauses.
- Partial-match filters are translated into SQL `ILIKE` clauses.
- Filtering is performed using metadata from three related tables:
  - `phenotype_data`
  - `entries`
  - `layouts`
- Filters are applied only to tables containing the corresponding fields.
- If no phenotype-data filters are supplied, a default catch-all filter is used
  to retrieve phenotype records.
- Entry metadata are joined onto phenotype records using the `entry` field.
- Layout metadata are joined onto phenotype records using the `layout` field.
- By default, the returned table is reshaped using `unstack_data_table`,
  producing a wide-format phenotype matrix.
- When `keep_id_and_do_not_unstack=true`, the long-format database representation
  is returned unchanged.
- The resulting DataFrame may contain columns originating from the phenotype,
  entries, and layouts tables.
- Progress messages describing each query stage are displayed when
  `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df_all = download_phenotype_data();

julia> df_exp = download_phenotype_data(experiments=[df_all.experiment[1]], like_experiments=["exp"]);

julia> df_ent = download_phenotype_data(entry_types=[df_all.entry_type[1]], like_entry_types=["fam"]);

julia> nrow(df_all) > nrow(df_exp)
true

julia> nrow(df_all) > nrow(df_ent)
true
```
"""
function download_phenotype_data(;
    experiments::Vector{String} = String[],
    sites::Vector{String} = String[],
    treatments::Vector{String} = String[],
    measurements::Vector{String} = String[],
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    traits::Vector{String} = String[],
    like_experiments::Vector{String} = String[],
    like_sites::Vector{String} = String[],
    like_treatments::Vector{String} = String[],
    like_measurements::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    like_traits::Vector{String} = String[],
    replications::Vector{Int64} = Int64[],
    blocks::Vector{Int64} = Int64[],
    rows::Vector{Int64} = Int64[],
    cols::Vector{Int64} = Int64[],
    keep_id_and_do_not_unstack::Bool = false,
    verbose::Bool = false,
)::DataFrame
    # experiments::Vector{String} = String[]
    # sites::Vector{String} = String[]
    # treatments::Vector{String} = String[]
    # measurements::Vector{String} = String[]
    # entries::Vector{String} = String[]
    # species::Vector{String} = String[]
    # entry_types::Vector{String} = String[]
    # traits::Vector{String} = String[]
    # like_experiments::Vector{String} = String[]
    # like_sites::Vector{String} = String[]
    # like_treatments::Vector{String} = String[]
    # like_measurements::Vector{String} = String[]
    # like_entries::Vector{String} = String[]
    # like_species::Vector{String} = String[]
    # like_entry_types::Vector{String} = String["fam"]
    # like_traits::Vector{String} = String[]
    # replications::Vector{Int64} = Int64[]
    # blocks::Vector{Int64} = Int64[]
    # rows::Vector{Int64} = Int64[]
    # cols::Vector{Int64} = Int64[1]
    # keep_id_and_do_not_unstack::Bool = false
    # verbose::Bool = true
    args = Dict(
        "experiments" => experiments,
        "sites" => sites,
        "treatments" => treatments,
        "measurements" => measurements,
        "entries" => entries,
        "species" => species,
        "entry_types" => entry_types,
        "traits" => traits,
        "like_experiments" => like_experiments,
        "like_sites" => like_sites,
        "like_treatments" => like_treatments,
        "like_measurements" => like_measurements,
        "like_entries" => like_entries,
        "like_species" => like_species,
        "like_entry_types" => like_entry_types,
        "like_traits" => like_traits,
        "replications" => replications,
        "blocks" => blocks,
        "rows" => rows,
        "cols" => cols,
    )
    conn = dbconnect()
    fields_to_ignore = String["id", "name", "created_at", "updated_at"]
    fields_expected_phenotype_data =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "phenotype_data")), Regex("_id\$")=>"")
    fields_expected_entries =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "entries")), Regex("_id\$")=>"")
    fields_expected_layouts =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "layouts")), Regex("_id\$")=>"")
    # Query using phenotype_data filters only (we'll filter using entries and layouts later)
    if verbose
        println("(1/4) Querying using `phenotype_data` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_phenotype_data
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "phenotype_data", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "phenotype_data", field = field, filter_in = v))
        end
    end
    filters = if length(filters) == 0
        push!(filters, Filter(conn, table = "phenotype_data", field = "experiment", filter_like = "%"))
    else
        filters
    end
    df = query(conn, filters, verbose = verbose)
    # Filter df using entries table filters
    if verbose
        println("(2/4) Querying using `entries` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[5]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_entries
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "entries", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "entries", field = field, filter_in = v))
        end
    end
    filters = if length(filters) == 0
        push!(filters, Filter(conn, table = "entries", field = "species", filter_like = "%"))
    else
        filters
    end
    df_entries = query(conn, filters, verbose = verbose)
    rename!(df_entries, "name" => "entry")
    select!(df_entries, Not([:id, :notes, :created_at, :updated_at]))
    entries = unique(df_entries.entry)
    filter!(x -> x.entry ∈ entries, df)
    df = leftjoin(df_entries, df, on = :entry)
    # Filter df using layouts table filters
    if verbose
        println("(3/4) Querying using `layouts` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[5]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_layouts
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "layouts", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "layouts", field = field, filter_in = v))
        end
    end
    filters = if length(filters) == 0
        push!(filters, Filter(conn, table = "layouts", field = "replication", filter_greater_than = -1000))
    else
        filters
    end
    df_layouts = query(conn, filters, verbose = verbose)
    rename!(df_layouts, "name" => "layout")
    select!(df_layouts, Not([:id, :created_at, :updated_at]))
    layouts = unique(df_layouts.layout)
    filter!(x -> !ismissing(x.layout), df)
    filter!(x -> x.layout ∈ layouts, df)
    df = leftjoin(df_layouts, df, on = :layout)
    # Prepare the final dataframe
    if verbose
        println("(4/4) Preparing final table...")
    end
    df = if !keep_id_and_do_not_unstack
        df = unstack_data_table(df)
        select(df, Not("layout"))
    else
        df
    end
    close(conn)
    if verbose
        println("Done!")
    end
    df
end
