"""
    upload_trial_data!(
        conn::LibPQ.Connection;
        fname::String,
        missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
        species::Union{Nothing, String} = nothing,
        experiment::Union{Nothing, String} = nothing,
        treatment::Union{Nothing, String} = nothing,
        entry_type::Union{Nothing, String} = nothing,
        population_type::Union{Nothing, String} = nothing,
        relationship_type::Union{Nothing, String} = nothing,
        measurement_dates::Union{Nothing, Dict{String, String}} = nothing,
        verbose::Bool = true,
    )::Nothing

Load and insert trial phenotype data into the GenomicBreedingDB database.

This is the primary function for uploading phenotypic trial data. 
It handles the complete pipeline of data validation, transformation, and insertion into 
the database, including species, experiments, treatments, sites, measurements, layouts, 
entries, traits, and phenotype values.

# Arguments
- `conn::LibPQ.Connection`: Active database connection for data insertion
- `fname::String`: Path to the input data file (supports both Trial struct format and CSV)
- `missing_strings::Vector{String}`: Missing value strings (default: `["missing", "NA", "na", "N/A", "n/a", ""]`)
- Include the following arguments if they are not present in the input dta file as separate columns:
    + `species::Union{Nothing, String}`: Species name to associate with the trial data
    + `experiment::Union{Nothing, String}`: Experiment identifier
    + `treatment::Union{Nothing, String}`: Treatment name or identifier
    + `entry_type::Union{Nothing, String}`: Type of entries (i.e., "cultivar", "population", "individual", "family")
    + `population_type::Union{Nothing, String}`: Type of population (i.e., "cultivar", "population", "individual", "family")
    + `relationship_type::Union{Nothing, String}`: Type of relationships between entries
    + `measurement_dates::Union{Nothing, Dict{String, String}}`: Dictionary mapping measurement names to dates
- `verbose::Bool`: Enable detailed logging of processing steps (default: `true`)

# Returns

- `Nothing`

# Details

The function performs the following operations in sequence:

1. **Data Loading**: Reads trial data from file (supports GenomicBreedingIO Trial struct format or CSV)
2. **Validation**: Ensures all required columns are present
3. **Layout Parsing**: Extracts layout information (replication, block, row, column)
4. **Metadata Assignment**: Adds species, experiment, treatment, entry type, and population type information
5. **Database Insertion**: Inserts or updates reference tables (species, experiments, treatments, sites, measurements, layouts, entries, traits)
6. **Field Updates**: Associates measurements with dates and layouts with spatial coordinates
7. **Entry Relationships**: Inserts pedigree/relationship data between entries
8. **Trait Extraction**: Identifies numeric phenotypic traits
9. **Phenotype Data**: Inserts individual phenotypic measurements linked to entries, experiments, sites, treatments, layouts, and measurement dates

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname);

julia> conn = dbconnect();

julia> try upload_trial_data!(conn, fname=fname); catch; false; end
false

julia> upload_trial_data!(conn, fname=fname, species="Acacia neglecta", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");

julia> execute(conn, "SELECT * FROM species") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM entries") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM entry_relationships") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM experiments") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM sites") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM treatments") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM layouts") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM measurements") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM traits") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM phenotype_data") |> DataFrame |> nrow > 0
true

julia> close(conn); rm(fname);
```
"""
function upload_trial_data!(
    conn::LibPQ.Connection;
    fname::String,
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
    species::Union{Nothing,String} = nothing,
    experiment::Union{Nothing,String} = nothing,
    treatment::Union{Nothing,String} = nothing,
    entry_type::Union{Nothing,String} = nothing,
    population_type::Union{Nothing,String} = nothing,
    relationship_type::Union{Nothing,String} = nothing,
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # fname = simulate_trial()
    # missing_strings::Union{String, Char, Vector{String}, Vector{Char}} = ["missing", "NA", "na", "N/A", "n/a", ""]
    # species::String = "Lolium multiflorum"
    # experiment::String = "STR_trial-2026"
    # treatment::String = "control"; verbose::Bool = true
    # entry_type::Union{Nothing, String} = "family"
    # population_type::Union{Nothing, String} = "population"
    # relationship_type::Union{Nothing, String} = "parent_is"
    # measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # # measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); df = CSV.read(fname, DataFrame); [measurement_dates[x] = x for x in ["$x" for x in unique(df.measurements)]]
    # verbose::Bool = true
    if entry_type∉["cultivar", "population", "individual", "family"]
        error(
            "Invalid entry_type: \"$entry_type\". Choose from: [\"cultivar\", \"population\", \"individual\", \"family\"].",
        )
    end
    if population_type∉["cultivar", "population", "individual", "family"]
        error(
            "Invalid population_type: \"$population_type\". Choose from: [\"cultivar\", \"population\", \"individual\", \"family\"].",
        )
    end
    if relationship_type∉["member_of", "clone_of", "parent_is", "maternal_parent_is", "paternal_parent_is"]
        error(
            "Invalid relationship_type: \"$relationship_type\". Choose from: [\"member_of\", \"clone_of\", \"parent_is\", \"maternal_parent_is\", \"paternal_parent_is\"].",
        )
    end
    # Load the trial data which assumed by default to be in Trial struct delimited file format (see: https://genomicbreeding.github.io/GenomicBreedingIO.jl/stable/#GenomicBreedingIO.readdelimited-Tuple{Type{GenomicBreedingCore.Trials}})
    df = load_trial_df(fname, missing_strings = missing_strings)
    # Make sure we have all the required columns
    validate_trials(df)
    parse_layouts!(df)
    add_col!(df, col = "species", value = species)
    add_col!(df, col = "experiments", value = experiment)
    add_col!(df, col = "treatments", value = treatment)
    add_col!(df, col = "entry_types", value = entry_type)
    add_col!(df, col = "population_types", value = population_type)
    add_col!(df, col = "relationship_types", value = relationship_type)
    add_measurement_dates!(df; measurement_dates = measurement_dates)
    df[!, "notes_years_seasons"] = string.("seasons_", df.years, "-", df.seasons)
    # Insert layout information
    insert_layouts!(conn, df = df)
    # Insert the names if they do not yet exist
    insert_names!(conn, df = df, table = "species", df_col = "species", verbose = verbose)
    insert_names!(conn, df = df, table = "experiments", df_col = "experiments", verbose = verbose)
    insert_names!(conn, df = df, table = "treatments", df_col = "treatments", verbose = verbose)
    insert_names!(conn, df = df, table = "sites", df_col = "sites", verbose = verbose)
    insert_names!(conn, df = df, table = "measurements", df_col = "measurements", verbose = verbose)
    insert_names!(conn, df = df, table = "entries", df_col = "entries", verbose = verbose)
    insert_names!(conn, df = df, table = "entries", df_col = "populations", verbose = verbose)
    # Update the measurement dates
    update_table_field_by_name!(
        conn,
        df = df,
        table = "measurements",
        df_name_col = "measurements",
        df_source_col = "dates",
        table_destination_field = "measure_date",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "measurements",
        df_name_col = "measurements",
        df_source_col = "notes_years_seasons",
        table_destination_field = "notes",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "entries",
        df_source_col = "entry_types",
        table_destination_field = "entry_type",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "populations",
        df_source_col = "population_types",
        table_destination_field = "entry_type",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "entries",
        df_source_col = "species",
        table_destination_field = "species_id",
        verbose = verbose,
    )
    insert_entry_relationships!(conn, df = df, verbose = verbose)
    # Extract the traits, i.e. numeric fields which are not layout or dates fields
    traits = extract_traits(df, verbose = verbose)
    insert_names!(conn, df = DataFrame(traits = traits), table = "traits", df_col = "traits", verbose = verbose)
    # Finally, insert/update the phenotype data using the combinations of the ids each entry-experiment-site-treatment-layout-measurement combinations
    insert_phenotype_data!(conn, df = df, traits = traits, verbose = verbose)
    nothing
end


"""
    upload_environment_data!(
        conn::LibPQ.Connection;
        fname::String,
        missing_strings::Vector{String}=[
            "missing",
            "NA",
            "na",
            "N/A",
            "n/a",
            "",
        ],
        experiment::Union{Nothing,String}=nothing,
        treatment::Union{Nothing,String}=nothing,
        measurement_dates::Union{Nothing,Dict{String,String}}=nothing,
        verbose::Bool=false,
    )::Nothing

Load environmental data from a file and upload it to the database.

This function provides a high-level workflow for importing environmental data.
It loads an environmental data file, standardises required metadata columns,
identifies environmental variables, updates lookup tables as needed, and
inserts the resulting observations into the `environment_data` table.

# Arguments

- `conn::LibPQ.Connection`: An open PostgreSQL connection.
- `fname::String`: Path to an environmental data file.

# Keyword Arguments

- `missing_strings::Vector{String}`: Strings that should be interpreted as
  missing values when importing the file.
- `experiment::Union{Nothing,String}=nothing`: Experiment name to assign to
  all observations.
- `treatment::Union{Nothing,String}=nothing`: Treatment name to assign to all
  observations.
- `measurement_dates::Union{Nothing,Dict{String,String}}=nothing`: Dictionary
  mapping measurement names to measurement dates. Passed directly to
  `add_measurement_dates!()`.
- `verbose::Bool=false`: If `true`, displays progress and summary information
  during import.

# Required Columns

The input file must contain the following columns:

- `measurements`
- `sites`

# Optional Columns

The following layout columns are optional:

- `replications`
- `blocks`
- `rows`
- `cols`

If any of these columns are missing, they are automatically added and assigned
the value `"1"` for all rows.

# Details

The function performs the following steps:

1. Loads the environmental data using `load_environment_df()`.
2. Verifies that the required columns `measurements` and `sites` are present.
3. Ensures the layout columns `replications`, `blocks`, `rows`, and `cols`
   exist.
4. Adds `experiments` and `treatments` columns using the supplied keyword
   arguments.
5. Adds measurement-date information using `add_measurement_dates!()`.
6. Identifies environmental-variable columns using
   `extract_environmental_variables()`.
7. Inserts any missing records into the:
   - `layouts`
   - `experiments`
   - `treatments`
   - `sites`
   - `measurements`
   - `environment_variables`
   
   tables.
8. Inserts environmental observations into the `environment_data` table using
   `insert_environment_data!()`.

Existing records are ignored where appropriate through the underlying insert
functions.

# Returns

`Nothing`.

# Throws

- `ErrorException`: If `fname` does not exist.
- `ErrorException`: If either the `measurements` or `sites` column is missing.
- Any exception generated by file loading, environmental-variable extraction,
  database insertion, or transaction handling in the underlying functions.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> fname_trial = simulate_trial(); fname_environment = simulate_environment(fname_trial);

julia> upload_environment_data!(conn, fname=fname_environment, experiment="exp-1", treatment="trt-42");

julia> execute(conn, "SELECT id,value FROM environment_data") |> DataFrame |> nrow > 0
true

julia> close(conn); rm.([fname_trial, fname_environment]);
```
"""
function upload_environment_data!(
    conn::LibPQ.Connection;
    fname::String,
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
    experiment::Union{Nothing,String} = nothing,
    treatment::Union{Nothing,String} = nothing,
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect(); fname = simulate_trial() |> simulate_environment; missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]; experiment="some-exp"; treatment="some_trt"; df = load_environment_df(fname, missing_strings=missing_strings); measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in [string(x) for x in unique(df.measurements)]]; verbose::Bool = true
    df = load_environment_df(fname, missing_strings = missing_strings)
    if length(names(df) ∩ ["measurements", "sites"]) != 2
        error(
            "The environment data file: \"$fname\" is missing one or more of these columns: [\"measurements\", \"sites\"].",
        )
    end
    spatial_cols = names(df) ∩ ["replications", "blocks", "rows", "cols"]
    "replications"∉spatial_cols ? add_col!(df, col = "replications", value = "1") : nothing
    "blocks"∉spatial_cols ? add_col!(df, col = "blocks", value = "1") : nothing
    "rows"∉spatial_cols ? add_col!(df, col = "rows", value = "1") : nothing
    "cols"∉spatial_cols ? add_col!(df, col = "cols", value = "1") : nothing
    add_col!(df, col = "experiments", value = experiment)
    add_col!(df, col = "treatments", value = treatment)
    add_measurement_dates!(df; measurement_dates = measurement_dates)
    environment_variables = extract_environment_variables(df, verbose = verbose)
    # Upload/update the database
    insert_layouts!(conn, df = df, is_trial = false)
    insert_names!(conn, df = df, table = "experiments", df_col = "experiments", verbose = verbose)
    insert_names!(conn, df = df, table = "treatments", df_col = "treatments", verbose = verbose)
    insert_names!(conn, df = df, table = "sites", df_col = "sites", verbose = verbose)
    insert_names!(conn, df = df, table = "measurements", df_col = "measurements", verbose = verbose)
    insert_names!(
        conn,
        df = DataFrame(environment_variables = environment_variables),
        table = "environment_variables",
        df_col = "environment_variables",
        verbose = verbose,
    )
    insert_environment_data!(conn, df = df, environment_variables = environment_variables)
    nothing
end

# TODO:
# 1.) Upload reference genomes
# 2.) Upload VCFs
# 3.) Upload Genomes
# 4.) Upload Phenomes
# 5.) Upload Fits
# 6.) Generate the relationship tables:
#   - genome_entries
#   - phenome_entries
#   - phenome_experiments
#   - phenome_sites
#   - phenome_treatments
#   - phenome_measurements
#   - phenome_traits