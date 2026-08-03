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
    pb = ProgressMeter.Progress(nrow(df)*length(traits), desc = "Importing phenotype data...")
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
  `check_trials`.
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
    # simulate_genomes() |> simulate_trials
    # fname = "simulated_trials.tsv"
    # missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]
    # species::String = "Lolium multiflorum"
    # experiment::String = "STR_trial-2026"
    # treatment::String = "control"; verbose::Bool = true
    # entry_type::Union{Nothing, String} = "family"
    # population_type::Union{Nothing, String} = "population"
    # relationship_type::Union{Nothing, String} = "parent_is"
    # measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # # measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); df = CSV.read(fname, DataFrame); [measurement_dates[x] = x for x in ["$x" for x in unique(df.measurements)]]
    # verbose::Bool = true
    # Load the trial data which assumed by default to be in Trial struct delimited file format (see: https://genomicbreeding.github.io/GenomicBreedingIO.jl/stable/#GenomicBreedingIO.readdelimited-Tuple{Type{GenomicBreedingCore.Trials}})
    df = load_trial_df(fname, missing_strings = missing_strings)
    # Make sure we have all the required columns
    check_trials(df)
    check(df, "entry_types", ["cultivar", "population", "individual", "family"], proposed_name = entry_type)
    check(df, "population_types", ["cultivar", "population", "individual", "family"], proposed_name = population_type)
    check(
        df,
        "relationship_types",
        ["member_of", "clone_of", "parent_is", "maternal_parent_is", "paternal_parent_is"],
        proposed_name = relationship_type,
    )
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
        table_destination_field = "note",
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
        note::String,
        link_value_parser_traits::Union{Nothing,Function}=nothing,
        link_value_parser_sites::Union{Nothing,Function}=nothing,
        link_value_parser_experiments::Union{Nothing,Function}=nothing,
        link_value_parser_measurements::Union{Nothing,Function}=nothing,
        link_value_parser_treatments::Union{Nothing,Function}=nothing,
        verbose::Bool=false,
    )::Nothing

Register a `Phenomes` dataset in the database and optionally create associated
relationship records.

The function validates a `Phenomes` JLD2 file, registers it in the `phenomes`
table, and optionally populates one or more phenotype relationship tables using
user-defined parsing functions.

A relationship between the uploaded `Phenomes` object and its entries is always
created via the `phenomes_entries` table. Additional relationships to traits,
sites, experiments, measurements, and treatments may also be created when the
corresponding `link_value_parser_*` functions are supplied.

This design supports phenotype datasets whose trait names encode multiple pieces
of metadata. For example, trait names may contain embedded site, treatment,
measurement, or experiment information that can be extracted and linked to
existing database records during upload.

If the phenotype dataset has already been registered, the database record is
left unchanged and a warning is emitted. Relationship records are then updated
using `define_relationships!`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to a valid `Phenomes` JLD2 file.
- `name::String`: Name assigned to the uploaded phenotype dataset.
- `note::String`: Descriptive note associated with the dataset.
- `link_value_parser_traits::Union{Nothing,Function}=nothing`: Function used to
  extract trait names from the `trait` field of the `Phenomes` struct 
  when populating the `phenomes_traits` relationship table.
- `link_value_parser_sites::Union{Nothing,Function}=nothing`: Function used to
  extract site names from `trait` the field of the `Phenomes` struct 
  when populating the `phenomes_sites` relationship table.
- `link_value_parser_experiments::Union{Nothing,Function}=nothing`: Function
  used to extract experiment names from the `trait` field of the `Phenomes` struct 
  when populating the `phenomes_experiments` relationship table.
- `link_value_parser_measurements::Union{Nothing,Function}=nothing`: Function
  used to extract measurement names from the `trait` field of the `Phenomes` struct 
  when populating the `phenomes_measurements` relationship table.
- `link_value_parser_treatments::Union{Nothing,Function}=nothing`: Function
  used to extract treatment names from the `trait` field of the `Phenomes` struct 
  when populating the `phenomes_treatments` relationship table.
- `verbose::Bool=false`: If `true`, display progress information whilst
  creating relationship records.

# Returns

- `Nothing`: The dataset is registered in the database and relationship records
  are created as requested.

# Throws

- `ErrorException`: If `fname` does not contain a valid `Phenomes` object.
- `ErrorException`: If `fname` is not an absolute file path.
- Any database exception raised whilst inserting records or querying metadata.
- Any exception raised by `define_relationships!`.
- Any exception raised by user-supplied parsing functions.

# Warnings

- A warning is emitted if the phenotype dataset has already been registered in
  the `phenomes` table.
- Existing records are preserved through the use of
  `INSERT ... ON CONFLICT DO NOTHING`.
- Relationship values parsed by the supplied parser functions must correspond
  to records that already exist in the appropriate database tables.

# Notes

- Validation of the input file is performed using
  `check(Phenomes; fname=fname)`.
- The phenotype file path must be supplied as an absolute path.
- Dataset records are inserted into the `phenomes` table with:
  - `name`
  - `file_path`
  - `note`
- Duplicate dataset registrations are ignored using
  `ON CONFLICT DO NOTHING`.
- A relationship between the uploaded dataset and its entries is always created
  using the `phenomes_entries` table.
- Additional relationship tables are only populated when the corresponding
  `link_value_parser_*` argument is supplied.
- Relationship creation is performed using `define_relationships!`.
- Parser functions receive the original trait string and must return the name
  of the related database entity to be linked.
- This mechanism enables trait identifiers containing embedded metadata to be
  decomposed into multiple database relationships.
- For example, a trait identifier such as:
  `yield|site_1|control|2024`
  could be parsed into separate trait, site, treatment, or experiment
  relationships.
- When `verbose=true`, progress information generated by
  `define_relationships!` is displayed.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_phenomes_jld2 = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2);

julia> conn = dbconnect(); 

julia> upload_phenomes!(conn, fname=abspath(fname_phenomes_jld2), name=fname_phenomes_jld2, note="simulated");

julia> query(conn, [Filter(conn, table="phenomes", field="name", filter_in=[fname_phenomes_jld2])]) |> nrow == 1
true

julia> fname_phenomes_jld2_NEW = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2_NEW);

julia> link_value_parser_traits = x -> String(split(x, '|')[1]);

julia> link_value_parser_sites = x -> String(split(split(x, '|')[2], "-")[end-1]);

julia> link_value_parser_experiments = x -> String("simulated experiment");

julia> link_value_parser_measurements = x -> String(join(split(split(x, '|')[2], "-")[2:4], "-"));

julia> link_value_parser_treatments = x -> String("control");

julia> upload_phenomes!(conn, fname=abspath(fname_phenomes_jld2_NEW), name=fname_phenomes_jld2_NEW, note="simulated", link_value_parser_traits=link_value_parser_traits, link_value_parser_sites=link_value_parser_sites, link_value_parser_experiments=link_value_parser_experiments, link_value_parser_measurements=link_value_parser_measurements, link_value_parser_treatments=link_value_parser_treatments);

julia> query(conn, [Filter(conn, table="phenomes", field="name", filter_in=[fname_phenomes_jld2_NEW])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_phenomes!(
    conn::LibPQ.Connection;
    fname::String,
    name::String,
    note::String,
    link_value_parser_traits::Union{Nothing,Function} = nothing,
    link_value_parser_sites::Union{Nothing,Function} = nothing,
    link_value_parser_experiments::Union{Nothing,Function} = nothing,
    link_value_parser_measurements::Union{Nothing,Function} = nothing,
    link_value_parser_treatments::Union{Nothing,Function} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect(); fname = string(pwd(), "/simulated_phenomes-", Dates.now(), ".jld2"); simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname); name = replace(fname, ".tsv" => ""); note = "simulated phenomes";
    # link_value_parser_traits = x -> String(split(x, '|')[1])
    # link_value_parser_sites = x -> String(split(split(x, '|')[2], "-")[end-1])
    # link_value_parser_experiments = x -> String("simulated experiment")
    # link_value_parser_measurements = x -> String(join(split(split(x, '|')[2], "-")[2:4], "-"))
    # link_value_parser_treatments = x -> String("control")
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
            note
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT DO NOTHING
        """,
        [name, fname, note],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname\" already exists!"
    end
    # Define relationship tables
    define_relationships!(conn, table = "phenomes_entries", fname_jld2_or_vcf = fname, verbose = verbose)
    if !isnothing(link_value_parser_traits)
        define_relationships!(
            conn,
            table = "phenomes_traits",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_traits,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_sites)
        define_relationships!(
            conn,
            table = "phenomes_sites",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_sites,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_experiments)
        define_relationships!(
            conn,
            table = "phenomes_experiments",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_experiments,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_measurements)
        define_relationships!(
            conn,
            table = "phenomes_measurements",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_measurements,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_treatments)
        define_relationships!(
            conn,
            table = "phenomes_treatments",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_treatments,
            verbose = verbose,
        )
    end
    # execute(conn, "SELECT * FROM phenomes") |> DataFrame
    nothing
end
