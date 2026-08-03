"""
    download_genotype_data_paths(
        table::String;
        names::Vector{String}=String[],
        notes::Vector{String}=String[],
        reference_genomes::Vector{String}=String[],
        entries::Vector{String}=String[],
        species::Vector{String}=String[],
        entry_types::Vector{String}=String[],
        like_names::Vector{String}=String[],
        like_notes::Vector{String}=String[],
        like_reference_genomes::Vector{String}=String[],
        like_entries::Vector{String}=String[],
        like_species::Vector{String}=String[],
        like_entry_types::Vector{String}=String[],
        verbose::Bool=false,
    )::DataFrame

Retrieve genotype-related dataset records matching exact-match and fuzzy-search
criteria.

The function searches one of the supported genotype-related database tables and
returns records whose metadata match the supplied filtering criteria.
Supported tables include `reference_genomes`, `genotype_vcfs`, and `genomes`.

Filtering may be performed using dataset metadata, associated reference
genomes, entries, species, and entry types. When multiple fuzzy-search terms
are supplied, all possible combinations are evaluated independently and the
final result is obtained by taking the union of all matching records.

This function is primarily intended for discovering registered genotype
resources and their associated file paths without loading the underlying data
files.

# Arguments

- `table::String`: Target database table. Must be one of:
  - `reference_genomes`
  - `genotype_vcfs`
  - `genomes`
- `names::Vector{String}=String[]`: Dataset names to match exactly.
- `notes::Vector{String}=String[]`: Dataset notes to match exactly.
- `reference_genomes::Vector{String}=String[]`: Reference genome names to
  match exactly.
- `entries::Vector{String}=String[]`: Entry names to match exactly.
- `species::Vector{String}=String[]`: Species names to match exactly.
- `entry_types::Vector{String}=String[]`: Entry types to match exactly.
- `like_names::Vector{String}=String[]`: Dataset name patterns for partial
  matching.
- `like_notes::Vector{String}=String[]`: Dataset-note patterns for partial
  matching.
- `like_reference_genomes::Vector{String}=String[]`: Reference-genome name
  patterns for partial matching.
- `like_entries::Vector{String}=String[]`: Entry name patterns for partial
  matching.
- `like_species::Vector{String}=String[]`: Species name patterns for partial
  matching.
- `like_entry_types::Vector{String}=String[]`: Entry-type patterns for partial
  matching.
- `verbose::Bool=false`: If `true`, display progress information during query
  execution.

# Returns

- `DataFrame`: Records matching the supplied filtering criteria.

# Throws

- `ErrorException`: If `table` is not one of the supported genotype-related
  tables.
- Any exception raised whilst constructing fuzzy-search combinations.
- Any exception raised during database querying.

# Notes

- A database connection is opened automatically and closed before returning.
- Supported tables are:
  - `reference_genomes`
  - `genotype_vcfs`
  - `genomes`
- Query construction and execution are delegated to
  `query(conn, args; ...)`.
- Exact-match filters are translated into SQL `IN` filters.
- Fuzzy-search filters are translated into SQL `ILIKE` filters.
- Multiple values supplied to `like_*` arguments are treated as independent
  search terms.
- When multiple fuzzy-search arguments contain multiple values, all possible
  combinations are evaluated using a Cartesian-product approach.
- Each fuzzy-search combination is executed independently and matching records
  are combined using a union operation.
- Duplicate records arising from multiple matching combinations are removed
  before returning the final result.
- For `genotype_vcfs` and `genomes`, filtering may be performed using:
  - dataset names,
  - notes,
  - reference genomes,
  - entries,
  - species, and
  - entry types.
- Filtering by entries, species, and entry types is performed through the
  corresponding relationship tables and metadata tables.
- For `reference_genomes`, entry-based and genotype-specific filters are not
  applicable and are ignored automatically.
- If no filters are supplied, a catch-all query based on
  `file_path LIKE '%'` is used, returning all records from the selected table.
- Returned records typically include:
  - `id`
  - `name`
  - `file_path`
  - `notes`
  - associated metadata fields
  - creation and update timestamps
- The function returns database metadata only and does not parse or load the
  underlying genotype files.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df = download_genotype_data_paths("reference_genomes");

julia> nrow(df) > 0
true

julia> df = download_genotype_data_paths("genotype_vcfs");

julia> nrow(df) > 0
true

julia> df = download_genotype_data_paths("genomes");

julia> nrow(df) > 0
true
```
"""
function download_genotype_data_paths(
    table::String;
    names::Vector{String} = String[],
    notes::Vector{String} = String[],
    reference_genomes::Vector{String} = String[],
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    like_names::Vector{String} = String[],
    like_notes::Vector{String} = String[],
    like_reference_genomes::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    verbose::Bool = false,
)::DataFrame
    # names = String[]; notes = String[]; reference_genomes = String[]; entries = String[]; species = String[]; entry_types = String[]; 
    # like_names = String[]; like_notes = String[]; like_reference_genomes = String[]; like_entries = String["_01", "_02"]; like_species = String[]; like_entry_types = String[]; 
    # table = "reference_genomes"; like_names = ["genome", "g"]; like_notes = ["data"]; like_reference_genomes = []; verbose = true
    # table = "genotype_vcfs"; like_names = ["genome", "g"]; like_notes = ["sim"]; like_reference_genomes = []; verbose = true
    # table = "genomes"; like_names = ["genome", "g"]; like_notes = []; like_reference_genomes = ["2026"]; verbose = true
    # extract_table_contents(dbconnect(), table)
    valid_tables = ["reference_genomes", "genotype_vcfs", "genomes"]
    if table∉valid_tables
        error("Invalid table! We expect one of these: [\"$(join(valid_tables, "\", \""))\"].")
    end
    conn = dbconnect()
    args = Dict(
        "names" => names,
        "notes" => notes,
        "reference_genomes" => reference_genomes,
        "entries" => entries,
        "species" => species,
        "entry_types" => entry_types,
        "like_names" => like_names,
        "like_notes" => like_notes,
        "like_reference_genomes" => like_reference_genomes,
        "like_entries" => like_entries,
        "like_species" => like_species,
        "like_entry_types" => like_entry_types,
    )
    if table == "reference_genomes"
        for k in [
            "entries",
            "species",
            "entry_types",
            "like_names",
            "like_notes",
            "like_reference_genomes",
            "like_entries",
            "like_species",
            "like_entry_types",
        ]
            delete!(args, k)
        end
    end
    like_combinations, like_combinations_keys = combinations(args)
    df_out = nothing
    for like_combination in like_combinations
        # like_combination = like_combinations[1]
        if !isnothing(like_combination)
            for (j, key) in enumerate(like_combinations_keys)
                # j = 1; key = like_combinations_keys[j]
                args[key] = [like_combination[j]]
            end
        end
        df = query(conn, args; table = table, backup_field_string = "file_path", verbose = verbose)
        df_out = if isnothing(df_out)
            df
        else
            unique(vcat(df_out, df))
        end
    end
    close(conn)
    df_out
end
