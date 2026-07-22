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
    upload_environment_data!(
        conn::LibPQ.Connection;
        fname::String,
        missing_strings::Vector{String}=[
            "missing", "NA", "na", "N/A", "n/a", ""
        ],
        experiment::Union{Nothing,String}=nothing,
        treatment::Union{Nothing,String}=nothing,
        measurement_dates::Union{Nothing,Dict{String,String}}=nothing,
        verbose::Bool=false,
    )::Nothing

Upload environmental data from a file into the database and populate all associated
reference and environmental data tables.

The function loads environmental data, validates the presence of required fields,
adds missing metadata where necessary, identifies environmental variables, and
imports all associated records into the database. This includes layouts,
experiments, treatments, sites, measurements, environmental variables, and
environmental observations.

Missing spatial layout columns are automatically populated with default values when
not present. Measurement dates may be supplied explicitly or inferred from
measurement identifiers.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Path to the environmental data file.
- `missing_strings::Vector{String}=["missing", "NA", "na", "N/A", "n/a", ""]`:
  Strings that should be interpreted as missing values when reading the file.
- `experiment::Union{Nothing,String}=nothing`: Experiment name to assign when an
  `experiments` column is not present in the input data.
- `treatment::Union{Nothing,String}=nothing`: Treatment name to assign when a
  `treatments` column is not present in the input data.
- `measurement_dates::Union{Nothing,Dict{String,String}}=nothing`: Optional
  mapping between measurement identifiers and measurement dates.
- `verbose::Bool=false`: If `true`, display progress information and diagnostic
  messages during processing.

# Returns

- `Nothing`: Environmental data are inserted into the database.

# Throws

- `ErrorException`: If the environmental data file cannot be loaded.
- `ErrorException`: If either the `measurements` or `sites` column is missing from
  the input data.
- `ErrorException`: If measurement dates are invalid or incomplete.
- `ErrorException`: If environmental variables cannot be identified.
- Any database exception raised during the import process.

# Notes

- Environmental data are loaded using `load_environments_df`.
- The columns `measurements` and `sites` are mandatory.
- Missing spatial columns (`replications`, `blocks`, `rows`, and `cols`) are
  automatically added with a default value of `"1"`.
- Missing experiment and treatment metadata may be added using `add_col!`.
- Measurement dates are validated or generated using
  `add_measurement_dates!`.
- Environmental variables are detected automatically using
  `extract_environment_variables`.
- Layout information is standardised and uploaded using `insert_layouts!`.
- Reference tables are populated using `insert_names!`.
- Environmental observations are inserted using
  `insert_environment_data!`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials |> simulate_environments;

julia> conn = dbconnect();

julia> upload_environment_data!(conn, fname="simulated_environments.tsv", experiment="exp-1", treatment="trt-42");

julia> execute(conn, "SELECT id,value FROM environment_data") |> DataFrame |> nrow > 0
true

julia> close(conn);
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
    # conn = dbconnect(); fname = simulate_trial() |> simulate_environment; missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]; experiment="some-exp"; treatment="some_trt"; df = load_environments_df(fname, missing_strings=missing_strings); measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in [string(x) for x in unique(df.measurements)]]; verbose::Bool = true
    df = load_environments_df(fname, missing_strings = missing_strings)
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

"""
    upload_reference_genome!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
    )::Nothing

Upload a reference genome file to the database and register its metadata.

The function validates that the supplied file exists and appears to be a valid
FASTA file before inserting a corresponding record into the
`reference_genomes` table. Both uncompressed FASTA files and gzip-compressed
FASTA files are supported.

Prior to insertion, the database is queried to ensure that the file path has not
already been registered. If the file has already been uploaded, a detailed error
message is returned describing the existing record. An additional consistency
check is performed to ensure that multiple records do not reference the same file
path.

Validation is performed by locating the first sequence record and confirming
that the sequence contains the canonical DNA nucleotide bases `A`, `T`, `C`,
and `G`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Path to the reference genome FASTA file.
- `name::String`: Unique name used to identify the reference genome.
- `notes::String`: Descriptive notes associated with the reference genome.

# Returns

- `Nothing`: The reference genome metadata are inserted into the database.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to contain valid FASTA-formatted
  sequence data.
- `ErrorException`: If the file path has already been registered in the
  database.
- `ErrorException`: If multiple database records reference the same file path.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a reference genome with the same name already exists
  in the database.

# Notes

- Both plain-text FASTA files and gzip-compressed FASTA files are supported.
- FASTA validation is based on inspection of the first detected sequence
  record.
- The function checks for the presence of the nucleotide bases `A`, `T`, `C`,
  and `G` in the sequence data.
- File-path uniqueness is verified before insertion using `query_table`.
- Metadata are inserted into the `reference_genomes` table using the supplied
  name, file path, and notes.
- Existing records with the same name are preserved through the use of
  `ON CONFLICT (name) DO NOTHING`.
- The genome file itself is not copied into the database; only its metadata and
  file location are recorded.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome);

julia> conn = dbconnect();

julia> upload_reference_genome!(conn, fname=fname_reference_genome, name=fname_reference_genome, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="reference_genomes", field="name", filter_in=[fname_reference_genome])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_reference_genome!(conn::LibPQ.Connection; fname::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname = string("simulated_reference_genome-", Dates.now(), ".fa"); simulate_reference_genome(fname_reference_genome=fname); name = "Milnesium tardigradum"; notes = "Simulated reference genome";
    if !isfile(fname)
        error("The reference genome file: \"$fname\" does not exist!")
    end
    line = String[""]
    try
        open(fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    catch
        open(CodecZlib.GzipDecompressorStream, fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    end
    if sum([x ∈ unique(collect(line[1])) for x in ['A', 'T', 'C', 'G']]) < 4
        error("The \"$fname\" may not be a fasta file!")
    end
    # Check if the file path has already been uploaded
    df_tmp = query_table(
        conn,
        filters = [Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname])],
    )
    if nrow(df_tmp) == 1
        info = string.(names(df_tmp), ": ", collect(df_tmp[1, :]))
        error(
            "The reference genome \"\" has already been uploaded with the following information:\n\t- $(join(info, "\n\t- "))",
        )
    end
    if nrow(df_tmp) > 1
        error("Catastropic error! We do not expect the same file (\"$fname\") to be in the database multiple times!")
    end
    res = execute(
        conn,
        """
        INSERT INTO reference_genomes
        (
            name,
            file_path,
            notes
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT (name) DO NOTHING
        """,
        [name, fname, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the FASTA file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM reference_genomes") |> DataFrame
    nothing
end

"""
    upload_genotype_vcf!(
        conn::LibPQ.Connection;
        fname_reference_genome::String,
        fname_genomes_vcf::String,
        name::String,
        notes::String,
        name_reference_genome::String="TBD",
        notes_reference_genome::String="TBD",
    )::Nothing

Upload a genotype VCF file to the database and associate it with a reference
genome.

The function validates the supplied reference genome and VCF files, ensures that
the reference genome has been registered in the database, and creates a
corresponding record in the `genotype_vcfs` table. If the reference genome has not
previously been uploaded, it is registered automatically before the VCF metadata
are inserted.

Validation is performed by inspecting the VCF header and confirming the presence of
the mandatory `#CHROM` line. Both uncompressed and gzip-compressed VCF files are
supported.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname_reference_genome::String`: Path to the reference genome FASTA file.
- `fname_genomes_vcf::String`: Path to the genotype VCF file.
- `name::String`: Unique name used to identify the genotype dataset.
- `notes::String`: Descriptive notes associated with the genotype dataset.
- `name_reference_genome::String="TBD"`: Name assigned to the reference genome
  record if it must be uploaded.
- `notes_reference_genome::String="TBD"`: Descriptive notes associated with the
  reference genome record if it must be uploaded.

# Returns

- `Nothing`: Metadata describing the genotype VCF dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the reference genome file does not exist.
- `ErrorException`: If the VCF file does not appear to be a valid VCF file.
- `ErrorException`: If the associated reference genome cannot be resolved in the
  database.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a record for the supplied VCF file already exists in
  the database.

# Notes

- The associated reference genome is identified using its file path.
- If the reference genome is not already registered, it is uploaded using
  `upload_reference_genome!`.
- If `name_reference_genome == "TBD"`, a reference genome name is generated
  automatically from the reference genome and VCF filenames.
- Existing reference genome records are reused when available.
- Both plain-text and gzip-compressed VCF files are supported.
- VCF validation is based on detection of the mandatory `#CHROM` header line.
- The reference genome identifier is stored in the `genotype_vcfs` table.
- Existing records are preserved through the use of
  `ON CONFLICT (file_path) DO NOTHING`.
- The VCF file itself is not stored in the database; only its metadata, file path,
  and reference genome association are recorded.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_vcf = string("simulated_genotype_vcf-", Dates.now(),".vcf");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_vcf=fname_genomes_vcf);

julia> conn = dbconnect(); 

julia> upload_genotype_vcf!(conn, fname_reference_genome=fname_reference_genome, fname_genomes_vcf=fname_genomes_vcf, name=fname_genomes_vcf, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="genotype_vcfs", field="name", filter_in=[fname_genomes_vcf])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_genotype_vcf!(
    conn::LibPQ.Connection;
    fname_reference_genome::String,
    fname_genomes_vcf::String,
    name::String,
    notes::String,
    name_reference_genome::String = "TBD",
    notes_reference_genome::String = "TBD",
)::Nothing
    # conn = dbconnect(); fname_reference_genome = string("simulated_reference_genome-", Dates.now(), ".fa"); fname_genomes_vcf = string("simulated_genomes-", Dates.now(), ".vcf"); simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_vcf=fname_genomes_vcf); name = string("Simulated_VCF-", Dates.now()); notes = "Simulated reference genome"; name_reference_genome::String = "TBD"; notes_reference_genome::String = "TBD"
    if !isfile(fname_reference_genome)
        error("The reference genome file: \"$fname_reference_genome\" does not exist!")
    end
    filters = [Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname_reference_genome])]
    reference_genome_id = try
        name_reference_genome = if name_reference_genome == "TBD"
            string(fname_reference_genome, " for ", fname_genomes_vcf)
        else
            name_reference_genome
        end
        upload_reference_genome!(
            conn,
            fname = fname_reference_genome,
            name = name_reference_genome,
            notes = notes_reference_genome,
        )
        query_table(conn, filters = filters, output_fields = ["id"]).id[1]
    catch
        query_table(conn, filters = filters, output_fields = ["id"]).id[1]
    end
    line = [String[""]]
    open(fname_genomes_vcf, "r") do io
        while line[1][1] != "#CHROM"
            line[1] = split(readline(io), "\t")
            if collect(line[1][1])[1] != '#'
                break
            end
        end
    end
    if line[1][1] != "#CHROM"
        open(CodecZlib.GzipDecompressorStream, fname_genomes_vcf, "r") do io
            while line[1][1] != "#CHROM"
                line[1] = split(readline(io), "\t")
                if collect(line[1][1])[1] != '#'
                    break
                end
            end
        end
    end
    if line[1][1] != "#CHROM"
        error("The \"$fname_genomes_vcf\" may not be a VCF file!")
    end
    res = execute(
        conn,
        """
        INSERT INTO genotype_vcfs
        (
            name,
            file_path,
            reference_genome_id,
            notes
        )
        VALUES (\$1,\$2,\$3,\$4)
        ON CONFLICT (file_path) DO NOTHING
        """,
        [name, fname_genomes_vcf, reference_genome_id, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the VCF file \"$fname_genomes_vcf\" already exists!"
    end
    # execute(conn, "SELECT * FROM genotype_vcfs") |> DataFrame
    nothing
end

"""
    upload_genomes!(
        conn::LibPQ.Connection;
        fname_reference_genome::String,
        fname_genomes_jld2::String,
        name::String,
        notes::String,
        name_reference_genome::String="TBD",
        notes_reference_genome::String="TBD",
    )::Nothing

Upload a `Genomes` JLD2 file to the database and associate it with a reference
genome.

The function validates the supplied reference genome and JLD2 files, ensures that
the reference genome has been registered in the database, and creates a
corresponding record in the `genomes` table. If the reference genome has not
previously been uploaded, it is registered automatically before the genomic
dataset metadata are inserted.

Validation is performed by inspecting the JLD2 file contents and confirming the
presence of signatures indicating a Julia-generated HDF5-backed JLD2 file
containing a `Genomes` object.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname_reference_genome::String`: Path to the reference genome FASTA file.
- `fname_genomes_jld2::String`: Path to the JLD2 file containing a `Genomes`
  object.
- `name::String`: Unique name used to identify the genomic dataset.
- `notes::String`: Descriptive notes associated with the genomic dataset.
- `name_reference_genome::String="TBD"`: Name assigned to the reference genome
  record if it must be uploaded.
- `notes_reference_genome::String="TBD"`: Descriptive notes associated with the
  reference genome record if it must be uploaded.

# Returns

- `Nothing`: Metadata describing the genomic dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the reference genome file does not exist.
- `ErrorException`: If the JLD2 file does not appear to contain a valid
  `Genomes` object.
- `ErrorException`: If the associated reference genome cannot be resolved in the
  database.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a record for the supplied JLD2 file already exists in
  the database.

# Notes

- The associated reference genome is identified using its file path.
- If the reference genome is not already registered, it is uploaded using
  `upload_reference_genome!`.
- If `name_reference_genome == "TBD"`, a reference genome name is generated
  automatically from the reference genome and JLD2 filenames.
- Existing reference genome records are reused when available.
- JLD2 validation is based on detection of the strings `Julia`, `HDF5`, and
  `Genomes` within the file contents.
- The reference genome identifier is stored in the `genomes` table.
- Existing records are preserved through the use of
  `ON CONFLICT (file_path) DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata, file
  path, and reference genome association are recorded.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_jld2 = string("simulated_genotype_jld2-", Dates.now(),".jld2");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2);

julia> conn = dbconnect(); 

julia> upload_genomes!(conn, fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2, name=fname_genomes_jld2, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="genomes", field="name", filter_in=[fname_genomes_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_genomes!(
    conn::LibPQ.Connection;
    fname_reference_genome::String,
    fname_genomes_jld2::String,
    name::String,
    notes::String,
    name_reference_genome::String = "TBD",
    notes_reference_genome::String = "TBD",
)::Nothing
    # conn = dbconnect(); fname_reference_genome = string("simulated_reference_genome-", Dates.now(), ".fa"); fname_genomes_jld2 = string("simulated_genomes-", Dates.now(), ".jld2"); simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2); name = replace(fname_genomes_jld2, ".jld2" => ""); notes = "Simulated genomes JLD2"; name_reference_genome::String = "TBD"; notes_reference_genome::String = "TBD"
    if !isfile(fname_reference_genome)
        error("The reference genome file: \"$fname_reference_genome\" does not exist!")
    end
    filters = [Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname_reference_genome])]
    reference_genome_id = try
        name_reference_genome = if name_reference_genome == "TBD"
            string(fname_reference_genome, " for ", fname_genomes_jld2)
        else
            name_reference_genome
        end
        upload_reference_genome!(
            conn,
            fname = fname_reference_genome,
            name = name_reference_genome,
            notes = notes_reference_genome,
        )
        query_table(conn, filters = filters, output_fields = ["id"]).id[1]
    catch
        query_table(conn, filters = filters, output_fields = ["id"]).id[1]
    end
    tmp = open(fname_genomes_jld2, "r") do io
        read(io, 1_000) |> String
    end
    if isnothing(match(Regex("Julia"), tmp)) ||
       isnothing(match(Regex("HDF5"), tmp)) ||
       isnothing(match(Regex("Genomes"), tmp))
        error("The file \"$fname_genomes_jld2\" may not be a JLD2 file containing a Genomes struct!")
    end
    res = execute(
        conn,
        """
        INSERT INTO genomes
        (
            name,
            file_path,
            reference_genome_id,
            notes
        )
        VALUES (\$1,\$2,\$3,\$4)
        ON CONFLICT (file_path) DO NOTHING
        """,
        [name, fname_genomes_jld2, reference_genome_id, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname_genomes_jld2\" already exists!"
    end
    # execute(conn, "SELECT * FROM genomes") |> DataFrame
    nothing
end

"""
    upload_phenomes!(
        conn::LibPQ.Connection;
        fname_phenomes_jld2::String,
        name::String,
        notes::String,
    )::Nothing

Upload a `Phenomes` JLD2 file to the database and register its metadata.

The function validates that the supplied JLD2 file exists and appears to contain a
valid `Phenomes` object before inserting a corresponding record into the
`phenomes` table. Validation is performed by inspecting the file contents for
signatures indicating a Julia-generated HDF5-backed JLD2 file containing a
`Phenomes` structure.

Once validated, the file path and associated metadata are recorded in the
database.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname_phenomes_jld2::String`: Path to the JLD2 file containing a `Phenomes`
  object.
- `name::String`: Unique name used to identify the phenomic dataset.
- `notes::String`: Descriptive notes associated with the phenomic dataset.

# Returns

- `Nothing`: Metadata describing the phenomic dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to contain a valid `Phenomes`
  object.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a record for the supplied JLD2 file already exists in
  the database.

# Notes

- JLD2 validation is based on detection of the strings `Julia`, `HDF5`, and
  `Phenomes` within the file contents.
- Metadata are inserted into the `phenomes` table using the supplied name, file
  path, and notes.
- Existing records are preserved through the use of
  `ON CONFLICT (file_path) DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata and file
  location are recorded.
- The function is intended for registering previously generated phenomic datasets
  rather than creating new ones.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_phenomes_jld2 = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2);

julia> conn = dbconnect(); 

julia> upload_phenomes!(conn, fname_phenomes_jld2=fname_phenomes_jld2, name=fname_phenomes_jld2, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="phenomes", field="name", filter_in=[fname_phenomes_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_phenomes!(conn::LibPQ.Connection; fname_phenomes_jld2::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname_phenomes_jld2 = string("simulated_phenomes-", Dates.now(), ".jld2"); simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2); name = replace(fname_phenomes_jld2, ".tsv" => ""); notes = "simulated phenomes";
    if !isfile(fname_phenomes_jld2)
        error("The phenomes file: \"$fname_phenomes_jld2\" does not exist!")
    end
    tmp = open(fname_phenomes_jld2, "r") do io
        read(io, 1_000) |> String
    end
    if isnothing(match(Regex("Julia"), tmp)) ||
       isnothing(match(Regex("HDF5"), tmp)) ||
       isnothing(match(Regex("Phenomes"), tmp))
        error("The file \"$fname_genomes_jld2\" may not be a JLD2 file containing a Phenomes struct!")
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
        ON CONFLICT (file_path) DO NOTHING
        """,
        [name, fname_phenomes_jld2, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname_phenomes_jld2\" already exists!"
    end
    # execute(conn, "SELECT * FROM phenomes") |> DataFrame
    nothing
end

"""
    upload_fit!(
        conn::LibPQ.Connection;
        fname_fit_jld2::String,
        name::String,
        notes::String,
    )::Nothing

Upload a `Fit` JLD2 file to the database and register its metadata.

The function validates that the supplied JLD2 file exists and appears to contain a
valid `Fit` object before inserting a corresponding record into the `fits` table.
Validation is performed by inspecting the file contents for signatures indicating a
Julia-generated HDF5-backed JLD2 file containing a `Fit` structure.

Once validated, the file path and associated metadata are recorded in the
database.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname_fit_jld2::String`: Path to the JLD2 file containing a `Fit` object.
- `name::String`: Unique name used to identify the fitted model.
- `notes::String`: Descriptive notes associated with the fitted model.

# Returns

- `Nothing`: Metadata describing the fitted model are inserted into the database.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to contain a valid `Fit` object.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a record for the supplied JLD2 file already exists in
  the database.

# Notes

- JLD2 validation is based on detection of the strings `Julia`, `HDF5`, and
  `Fit` within the file contents.
- Metadata are inserted into the `fits` table using the supplied name, file path,
  and notes.
- Existing records are preserved through the use of
  `ON CONFLICT (file_path) DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata and file
  location are recorded.
- The function is intended for registering previously generated fitted models for
  subsequent retrieval and analysis.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_fit_jld2 = string("simulated_fit_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> simulate_phenomes;

julia> simulate_fit(genomes, phenomes, fname_fit_jld2=fname_fit_jld2);

julia> conn = dbconnect(); 

julia> upload_fit!(conn, fname_fit_jld2=fname_fit_jld2, name=fname_fit_jld2, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="fits", field="name", filter_in=[fname_fit_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_fit!(conn::LibPQ.Connection; fname_fit_jld2::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname_fit_jld2 = string("simulated_fit-", Dates.now(), ".jld2"); genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> simulate_phenomes; simulate_fit(genomes, phenomes, fname_fit_jld2=fname_fit_jld2); name = replace(fname_fit_jld2, ".tsv" => ""); notes = "simulated fit";
    if !isfile(fname_fit_jld2)
        error("The fit file: \"$fname_fit_jld2\" does not exist!")
    end
    tmp = open(fname_fit_jld2, "r") do io
        read(io, 1_000) |> String
    end
    if isnothing(match(Regex("Julia"), tmp)) ||
       isnothing(match(Regex("HDF5"), tmp)) ||
       isnothing(match(Regex("Fit"), tmp))
        error("The file \"$fname_genomes_jld2\" may not be a JLD2 file containing a Fit struct!")
    end
    res = execute(
        conn,
        """
        INSERT INTO fits
        (
            name,
            file_path,
            notes
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT (file_path) DO NOTHING
        """,
        [name, fname_fit_jld2, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname_fit_jld2\" already exists!"
    end
    # execute(conn, "SELECT * FROM fits") |> DataFrame
    nothing
end
