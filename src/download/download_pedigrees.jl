"""
    download_pedigrees(
        entries::Vector{String}=String[],
        species::Vector{String}=String[],
        entry_types::Vector{String}=String[],
        like_entries::Vector{String}=String[],
        like_species::Vector{String}=String[],
        like_entry_types::Vector{String}=String[],
        verbose::Bool=false,
    )::DataFrame

Download pedigree relationships associated with entries matching the supplied
filtering criteria.

The function identifies entries using the same filtering framework employed by
`download_phenotype_data` and then retrieves all pedigree relationships in which
those entries appear as either parents or children.

Relationships are obtained from the `entry_relationships` table and include both
upstream (parent) and downstream (child) connections. The resulting pedigree
records are merged and deduplicated before being returned.

This function provides a convenient mechanism for extracting pedigree
information for a subset of entries, species, or entry types without requiring
manual traversal of the relationship table.

# Arguments

- `entries::Vector{String}=String[]`: Entry names to match exactly.
- `species::Vector{String}=String[]`: Species names to match exactly.
- `entry_types::Vector{String}=String[]`: Entry types to match exactly.
- `like_entries::Vector{String}=String[]`: Entry name patterns for partial
  matching.
- `like_species::Vector{String}=String[]`: Species name patterns for partial
  matching.
- `like_entry_types::Vector{String}=String[]`: Entry-type patterns for partial
  matching.
- `verbose::Bool=false`: If `true`, display progress information during
  querying and pedigree extraction.

# Returns

- `DataFrame`: Pedigree relationships involving the matched entries.

# Throws

- Any exception raised by `download_phenotype_data`.
- Any exception raised whilst querying the database.
- Any exception raised whilst resolving entry identifiers.

# Notes

- Entry selection is performed using `download_phenotype_data`.
- Only entries represented in the filtered phenotype dataset are considered.
- Entry identifiers are retrieved from the `entries` table.
- Pedigree information is stored in the `entry_relationships` table.
- Relationships where a selected entry appears as a child are retrieved.
- Relationships where a selected entry appears as a parent are retrieved.
- Parent and child relationship records are combined and deduplicated before
  being returned.
- The returned DataFrame contains raw pedigree relationship records from the
  `entry_relationships` table.
- The `entry_relationships` table is queried directly because relationship
  identifiers are stored in the table itself and do not require metadata-table
  resolution.
- All database queries are performed using validated, sanitised, and
  parameterised filtering operations.
- When multiple fuzzy-search terms are supplied, their handling follows the
  behaviour implemented by `download_phenotype_data`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df = download_pedigrees();

julia> nrow(df) > 0
true
```
"""
function download_pedigrees(
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    verbose::Bool = false,
)::DataFrame
    # entries::Vector{String}=String[]
    # species::Vector{String}=String[]
    # entry_types::Vector{String}=String[]
    # like_entries::Vector{String}=String["_10", "_20"]
    # like_species::Vector{String}=String[]
    # like_entry_types::Vector{String}=String[]
    # verbose::Bool=true
    df_phenotype_data = download_phenotype_data(
        entries = entries,
        species = species,
        entry_types = entry_types,
        like_entries = like_entries,
        like_species = like_species,
        like_entry_types = like_entry_types,
        keep_id_and_do_not_unstack = false,
        verbose = verbose,
    )
    conn = dbconnect()
    df_entries = query(
        conn,
        [Filter(conn, table = "entries", field = "name", filter_in = string.(unique(df_phenotype_data.entry)))],
        verbose = verbose,
    )
    df_children = query(
        conn,
        [Filter(conn, table = "entry_relationships", field = "child_id", filter_in = string.(unique(df_entries.id)))],
        verbose = verbose,
    )
    df_parents = query(
        conn,
        [Filter(conn, table = "entry_relationships", field = "parent_id", filter_in = string.(unique(df_entries.id)))],
        verbose = verbose,
    )
    df = unique(vcat(df_children, df_parents))
    close(conn)
    df
end
