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
        values::Tuple{Float64,Float64}=(-Inf, +Inf),
        keep_id_and_do_not_unstack::Bool=false,
        verbose::Bool=false,
    )::DataFrame

Download phenotype data from the database using exact-match, partial-match,
and numeric-range filters.

The function retrieves records from the `phenotype_data` table and augments them
with metadata from the related `entries` and `layouts` tables. Filtering
criteria are supplied through keyword arguments and are automatically translated
into validated `Filter` objects and parameterised SQL queries.

Data retrieval is performed in three stages. First, phenotype observations are
filtered using fields from the `phenotype_data` table. Second, entry-level
metadata filters are applied using the `entries` table. Third, layout-level
filters are applied using the `layouts` table. The resulting datasets are joined
and optionally reshaped into a wide-format phenotype matrix suitable for
analysis.

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
- `like_experiments::Vector{String}=String[]`: Experiment name patterns for
  partial matching.
- `like_sites::Vector{String}=String[]`: Site name patterns for partial
  matching.
- `like_treatments::Vector{String}=String[]`: Treatment name patterns for
  partial matching.
- `like_measurements::Vector{String}=String[]`: Measurement name patterns for
  partial matching.
- `like_entries::Vector{String}=String[]`: Entry name patterns for partial
  matching.
- `like_species::Vector{String}=String[]`: Species name patterns for partial
  matching.
- `like_entry_types::Vector{String}=String[]`: Entry-type patterns for partial
  matching.
- `like_traits::Vector{String}=String[]`: Trait name patterns for partial
  matching.
- `replications::Vector{Int64}=Int64[]`: Replication identifiers to match.
- `blocks::Vector{Int64}=Int64[]`: Block identifiers to match.
- `rows::Vector{Int64}=Int64[]`: Plot-row identifiers to match.
- `cols::Vector{Int64}=Int64[]`: Plot-column identifiers to match.
- `values::Tuple{Float64,Float64}=(-Inf, +Inf)`: Inclusive lower and upper
  bounds used to filter phenotype values.
- `keep_id_and_do_not_unstack::Bool=false`: If `true`, return the original
  long-format representation instead of reshaping the data.
- `verbose::Bool=false`: If `true`, display progress messages throughout data
  retrieval and post-processing.

# Returns

- `DataFrame`: Phenotype data matching the supplied filtering criteria.

# Throws

- `ErrorException`: If one or more filters fail validation.
- `ErrorException`: If an invalid field, table, or filtering criterion is
  supplied.
- `ErrorException`: If an invalid numeric interval is supplied via `values`.
- Any database exception raised during querying, joining, or reshaping.

# Notes

- A database connection is opened automatically and closed before returning.
- Exact-match filters are translated into SQL `IN` filters.
- Partial-match filters are translated into SQL `ILIKE` filters.
- The `values` argument is translated into a `filter_between` constraint on the
  `phenotype_data.value` field.
- All generated filters are validated, type-checked, sanitised, and converted
  into parameterised SQL statements before execution.
- Query construction is delegated to the helper `query(conn, args; ...)`
  method, reducing duplicated filtering logic across tables.
- Filtering is performed using metadata from three related tables:
  - `phenotype_data`
  - `entries`
  - `layouts`
- Entry metadata are joined to phenotype observations through the `entry`
  field.
- Layout metadata are joined to phenotype observations through the `layout`
  field.
- Phenotype records without matching entry or layout metadata are excluded from
  the final result.
- By default, the returned table is reshaped using `unstack_data_table`,
  producing a wide-format phenotype matrix with traits as columns.
- When `keep_id_and_do_not_unstack=true`, the original long-format database
  representation is returned without reshaping. This is primarily useful for
  data-correction workflows, as database identifiers are retained and can be
  used to update specific records directly.
- The resulting DataFrame may contain fields originating from the
  `phenotype_data`, `entries`, and `layouts` tables.
- Progress messages describing each processing stage are displayed when
  `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df_all = download_phenotype_data();

julia> df_exp = download_phenotype_data(experiments=[df_all.experiment[1]], like_experiments=["exp"]);

julia> df_ent = download_phenotype_data(entry_types=["family"]);

julia> nrow(df_all) > nrow(df_exp)
true

julia> nrow(df_all) == nrow(df_ent)
true

julia> df_ent = download_phenotype_data(entry_types=["family"], like_entry_types=["pop"]);

julia> nrow(df_ent) == 0
true

julia> df_val = download_phenotype_data(values=(5., 10.));

julia> nrow(df_all) > nrow(df_val)
true

julia> df_lon = download_phenotype_data(keep_id_and_do_not_unstack=true);

julia> size(df_all) != size(df_lon)
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
    values::Tuple{Float64,Float64} = (-Inf, +Inf),
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
    # values::Tuple{Float64, Float64} = (0., 10.)
    # keep_id_and_do_not_unstack::Bool = false
    # verbose::Bool = true
    args::Dict{String, Union{Tuple{Float64, Float64}, Vector{Float64}, Vector{Int64}, Vector{String}}} = Dict(
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
        "values" => values,
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
    df = query(
        conn,
        args;
        table="phenotype_data",
        expected_fields=fields_expected_phenotype_data,
        backup_field_string="experiment",
        verbose=verbose,
    )
    # Filter df using entries table filters
    if verbose
        println("(2/4) Querying using `entries` table filters...")
    end
    df_entries = query(
        conn,
        args;
        table="entries",
        expected_fields=fields_expected_entries,
        backup_field_string="species",
        verbose=verbose,
    )
    rename!(df_entries, "name" => "entry")
    select!(df_entries, Not([:id, :notes, :created_at, :updated_at]))
    entries = unique(df_entries.entry)
    filter!(x -> x.entry ∈ entries, df)
    df = innerjoin(df_entries, df, on = :entry)
    # Filter df using layouts table filters
    if verbose
        println("(3/4) Querying using `layouts` table filters...")
    end
    df_layouts = query(
        conn,
        args;
        table="layouts",
        expected_fields=fields_expected_layouts,
        backup_field_numeric="replication",
        verbose=verbose,
    )
    rename!(df_layouts, "name" => "layout")
    select!(df_layouts, Not([:id, :created_at, :updated_at]))
    layouts = unique(df_layouts.layout)
    filter!(x -> !ismissing(x.layout), df)
    filter!(x -> x.layout ∈ layouts, df)
    df = innerjoin(df_layouts, df, on = :layout)
    # Prepare the final dataframe
    if verbose
        println("(4/4) Preparing final table...")
    end
    df = if !keep_id_and_do_not_unstack
        select(
            unstack_data_table(df),
            Not("layout")
        )
    else
        df
    end
    close(conn)
    if verbose
        println("Done!")
    end
    df
end
