"""
    insert_phenotype_data!(
        conn::LibPQ.Connection;
        df::DataFrame,
        traits::Vector{String},
        verbose::Bool=false,
    )::Nothing

Insert phenotype observations from a DataFrame into the `phenotype_data` database
table. This function is called by `upload_trial_data!(...)` which handles the input
file loading and DataFrame preparation.

The function resolves identifiers for entries, experiments, sites, treatments,
layouts, measurements, and traits before importing phenotype values into the
database. For each row in the input DataFrame and each specified trait, a phenotype
record is created linking the relevant experimental factors to the observed trait
value.

All insert operations are performed within a single transaction. Existing phenotype
records are ignored using an `ON CONFLICT DO NOTHING` clause, allowing repeated
imports without creating duplicate observations.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing phenotype observations and associated
  metadata.
- `traits::Vector{String}`: Names of trait columns to import as phenotype data.
- `verbose::Bool=false`: If `true`, display progress information during import.

# Returns

- `Nothing`: Phenotype records are inserted into the database.

# Throws

- `ErrorException`: If the `phenotype_data` table does not exist.
- `ErrorException`: If any required reference table has not been initialised.
- Any database exception raised during insertion is rethrown after the transaction
  is rolled back.

# Notes

- The following reference tables must be populated before import:
  `entries`, `experiments`, `sites`, `treatments`, `layouts`, `measurements`, and
  `traits`.
- Identifier values are resolved using `extract_ids` prior to data insertion.
- Missing phenotype values are stored as `NaN`.
- One database record is generated for each combination of DataFrame row and trait.
- Insert operations are wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Existing records are preserved through the use of a composite
  `ON CONFLICT DO NOTHING` constraint.
- Progress reporting is available when `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> conn = dbconnect();

julia> df.entries = string.("test_phenodat-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> [df[!, name] .= string("test_insert_phenodat_", name) for name in ["experiments", "treatments"]];

julia> parse_layouts!(df); insert_layouts!(conn, df=df);

julia> [insert_names!(conn, df=df, table=name, df_col=name) for name in ["entries", "experiments", "sites", "treatments", "measurements"]];

julia> traits = extract_traits(df); insert_names!(conn, df=DataFrame(traits=traits), table="traits", df_col="traits");

julia> insert_phenotype_data!(conn, df=df, traits=["trait_1"]);

julia> df_exported = execute(conn, "SELECT * FROM phenotype_data") |> DataFrame;

julia> ids = Dict(); [ids[x] = Dict("df" => extract_ids(conn, names=unique(df[!, x]), table=x), "name" => x == "entries" ? "entry" : join(collect(x)[1:(end-1)]) ) for x in [x == "entrys" ? "entries" : x for x in replace.(names(df_exported)[2:(end-4)], "_id" => "s")]];

julia> [v["df"] = rename(v["df"], ["id" => string(v["name"], "_id"), "name" => v["name"]]) for (k, v) in ids];

julia> for (k, v) in ids; df_exported = innerjoin(df_exported, v["df"], on=string(v["name"], "_id")); end

julia> ids_exported = ["experiment", "site", "treatment", "layout", "measurement", "entry", "value"]; select!(df_exported, ids_exported);

julia> ids = ["experiments", "sites", "treatments", "layouts", "measurements", "entries", "trait_1"]; select!(df, ids);

julia> df.trait_1[ismissing.(df.trait_1)] .= Inf;

julia> df_exported.value[isnan.(df_exported.value)] .= Inf;

julia> sort!(df_exported, ids_exported);

julia> sort!(df, ids);

julia> Matrix(df_exported) == Matrix(df)
true

julia> close(conn);
```
"""
function insert_phenotype_data!(conn::LibPQ.Connection; df::DataFrame, traits::Vector{String}, verbose::Bool = false)
    check(conn, "phenotype_data")
    tables = ["entries", "experiments", "sites", "treatments", "layouts", "measurements", "traits"]
    names_in_db::Dict{String,DataFrame} = Dict()
    errors = String[]
    for table in tables
        # table = tables[end]
        names_df = if table != "traits"
            String.(unique(df[!, table]))
        else
            traits
        end
        try
            names_in_db[table] = extract_ids(conn, names = names_df, table = table)
        catch
            push!(errors, "Please initialise the \"$table\" table!")
        end
    end
    if length(errors) > 0
        error(join(string.("\n\t- ", errors)))
    end
    pb = ProgressMeter.Progress(nrow(df)*length(traits), "Importing phenotype data...")
    execute(conn, "BEGIN")
    try
        for i = 1:nrow(df)
            # i = 7
            # println(i)
            entry_id = filter(x->x.name==df.entries[i], names_in_db["entries"]).id[1]
            experiment_id = filter(x->x.name==df.experiments[i], names_in_db["experiments"]).id[1]
            site_id = filter(x->x.name==df.sites[i], names_in_db["sites"]).id[1]
            treatment_id = filter(x->x.name==df.treatments[i], names_in_db["treatments"]).id[1]
            layout_id = filter(x->x.name==df.layouts[i], names_in_db["layouts"]).id[1]
            measurement_id = filter(x->x.name==df.measurements[i], names_in_db["measurements"]).id[1]
            for trait in traits
                # trait = traits[2]
                # println(trait)
                trait_id = filter(x->x.name==trait, names_in_db["traits"]).id[1]
                y = !ismissing(df[i, trait]) ? df[i, trait] : NaN
                # y = NaN
                execute(
                    conn,
                    """
                    INSERT INTO phenotype_data
                    (
                        entry_id,
                        experiment_id,
                        site_id,
                        treatment_id,
                        layout_id,
                        measurement_id,
                        trait_id,
                        value
                    )
                    VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8)
                    ON CONFLICT 
                    (
                        entry_id,
                        experiment_id,
                        site_id,
                        treatment_id,
                        layout_id,
                        measurement_id,
                        trait_id
                    ) DO NOTHING
                    """,
                    [entry_id, experiment_id, site_id, treatment_id, layout_id, measurement_id, trait_id, y],
                )
                verbose ? ProgressMeter.next!(pb) : nothing
            end
        end
        verbose ? ProgressMeter.finish!(pb) : nothing
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    # execute(conn, "SELECT value FROM phenotype_data") |> DataFrame
    nothing
end

"""
    upload_trial_data!(
        conn::LibPQ.Connection;
        fname::String,
        missing_strings::Vector{String}=[
            "missing", "NA", "na", "N/A", "n/a", ""
        ],
        species::Union{Nothing,String}=nothing,
        experiment::Union{Nothing,String}=nothing,
        treatment::Union{Nothing,String}=nothing,
        entry_type::Union{Nothing,String}=nothing,
        population_type::Union{Nothing,String}=nothing,
        relationship_type::Union{Nothing,String}=nothing,
        measurement_dates::Union{Nothing,Dict{String,String}}=nothing,
        verbose::Bool=false,
    )::Nothing

Upload trial data from a file into the database and populate all associated
reference and phenotype tables.

The function loads and validates trial data, standardises layout information,
augments missing metadata fields, and inserts or updates all required database
records. This includes layouts, species, experiments, treatments, sites,
measurements, entries, populations, entry relationships, traits, and phenotype
observations.

Measurement dates may be provided explicitly or inferred from measurement names.
Database reference tables are populated automatically where required, and
associated metadata fields are updated using name-based lookups.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Path to the trial data file.
- `missing_strings::Vector{String}=["missing", "NA", "na", "N/A", "n/a", ""]`:
  Strings that should be interpreted as missing values when reading the file.
- `species::Union{Nothing,String}=nothing`: Species name to assign when a
  `species` column is not present in the input data.
- `experiment::Union{Nothing,String}=nothing`: Experiment name to assign when an
  `experiments` column is not present.
- `treatment::Union{Nothing,String}=nothing`: Treatment name to assign when a
  `treatments` column is not present.
- `entry_type::Union{Nothing,String}=nothing`: Entry type assigned to entries when
  an `entry_types` column is not present.
- `population_type::Union{Nothing,String}=nothing`: Entry type assigned to
  populations when a `population_types` column is not present.
- `relationship_type::Union{Nothing,String}=nothing`: Relationship type assigned
  when a `relationship_types` column is not present.
- `measurement_dates::Union{Nothing,Dict{String,String}}=nothing`: Optional
  mapping between measurement identifiers and dates.
- `verbose::Bool=false`: If `true`, display progress information and summary
  messages during processing.

# Returns

- `Nothing`: Trial information is inserted into the database.

# Throws

- `ErrorException`: If `entry_type` is not one of the supported values.
- `ErrorException`: If `population_type` is not one of the supported values.
- `ErrorException`: If `relationship_type` is not one of the supported values.
- `ErrorException`: If the trial file cannot be loaded or fails validation.
- Any database exception raised during the import process.

# Notes

- Supported entry and population types are:
  `cultivar`, `population`, `individual`, and `family`.
- Supported relationship types are:
  `member_of`, `clone_of`, `parent_is`, `maternal_parent_is`, and
  `paternal_parent_is`.
- Trial data are loaded using `load_trial_df` and validated using
  `validate_trials`.
- Layout information is standardised using `parse_layouts!`.
- Missing metadata columns may be added automatically using `add_col!`.
- Measurement dates are validated or generated using
  `add_measurement_dates!`.
- Reference tables are populated using `insert_names!`.
- Existing measurement and entry metadata are updated using
  `update_table_field_by_name!`.
- Entry-to-population relationships are inserted using
  `insert_entry_relationships!`.
- Trait columns are detected automatically using `extract_traits`.
- Phenotype observations are inserted using `insert_phenotype_data!`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> conn = dbconnect();

julia> try upload_trial_data!(conn, fname="simulated_trials.tsv"); catch; false; end
false

julia> upload_trial_data!(conn, fname="simulated_trials.tsv", species="Acacia neglecta", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");

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

julia> close(conn);
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
    upload_phenomes!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
    )::Nothing

Upload a `Phenomes` dataset to the database and register its metadata.

The function validates that the supplied file exists and appears to be a JLD2 file
containing a `Phenomes` object, verifies that the file path is absolute, and then
registers the dataset in the `phenomes` table.

File validation is delegated to `check(Phenomes; fname=...)`, which performs a
lightweight inspection of the file to ensure it appears to contain a valid
`Phenomes` object. Existing records are preserved using an
`ON CONFLICT DO NOTHING` clause.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the JLD2 file containing a `Phenomes`
  object.
- `name::String`: Unique name used to identify the phenomic dataset.
- `notes::String`: Descriptive notes associated with the phenomic dataset.

# Returns

- `Nothing`: Metadata describing the phenomic dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the supplied file does not exist.
- `ErrorException`: If the file does not appear to contain a valid `Phenomes`
  object.
- `ErrorException`: If the file path is not absolute.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a matching phenomes record already exists and the
  insert operation is ignored.

# Notes

- File validation is delegated to `check(Phenomes; fname=...)`.
- Only absolute file paths are accepted.
- Records are inserted into the `phenomes` table using the supplied name, file
  path, and notes.
- Existing records are preserved through the use of
  `ON CONFLICT DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata and file
  location are recorded.
- This function is intended for registering previously generated phenomic
  datasets rather than creating new ones.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_phenomes_jld2 = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2);

julia> conn = dbconnect(); 

julia> upload_phenomes!(conn, fname=abspath(fname_phenomes_jld2), name=fname_phenomes_jld2, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="phenomes", field="name", filter_in=[fname_phenomes_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_phenomes!(conn::LibPQ.Connection; fname::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname = string(pwd(), "/simulated_phenomes-", Dates.now(), ".jld2"); simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname); name = replace(fname, ".tsv" => ""); notes = "simulated phenomes";
    check(Phenomes, fname = fname)
    if !isabspath(fname)
        error("The path to the Phenomes file is not absolute: \"$fname\"!")
    end
    res = execute(
        conn,
        """
        INSERT INTO phenomes
        (
            name,
            file_path,
            notes
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT DO NOTHING
        """,
        [name, fname, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM phenomes") |> DataFrame
    nothing
end
