"""
    upload(
        fname::String;
        missing_strings::Vector{String}=["missing", "NA", "na", "N/A", "n/a", ""],
        species::Union{Nothing,String}=nothing,
        experiment::Union{Nothing,String}=nothing,
        treatment::Union{Nothing,String}=nothing,
        entry_type::Union{Nothing,String}=nothing,
        population_type::Union{Nothing,String}=nothing,
        relationship_type::Union{Nothing,String}=nothing,
        measurement_dates::Union{Nothing,Dict{String,String}}=nothing,
        name::Union{Nothing,String}=nothing,
        note::Union{Nothing,String}=nothing,
        fname_reference_genome::Union{Nothing,String}=nothing,
        link_value_parser_traits::Union{Nothing,Function}=nothing,
        link_value_parser_sites::Union{Nothing,Function}=nothing,
        link_value_parser_experiments::Union{Nothing,Function}=nothing,
        link_value_parser_measurements::Union{Nothing,Function}=nothing,
        link_value_parser_treatments::Union{Nothing,Function}=nothing,
        verbose::Bool=false,
    )::Nothing

Automatically detect the type of a supported input file and upload its contents
to the database.

The function provides a unified interface for importing supported data types.
The supplied file is inspected using a series of validation and parsing
routines to determine its type before dispatching to the appropriate
specialised upload function.

Supported uploads include trial data, environmental data, reference genomes,
VCF files, `Genomes` objects, `Phenomes` objects, and `Fit` objects. Once the
file type has been identified, a database connection is established and the
corresponding upload workflow is executed automatically.

Depending on the detected file type, additional keyword arguments may be
required and are forwarded to the underlying upload function.

# Arguments

- `fname::String`: Path to the file to upload.
- `missing_strings::Vector{String}=["missing", "NA", "na", "N/A", "n/a", ""]`:
  Strings to interpret as missing values when importing tabular data.
- `species::Union{Nothing,String}=nothing`: Species name used when importing
  trial data.
- `experiment::Union{Nothing,String}=nothing`: Experiment name used when
  importing trial or environmental data.
- `treatment::Union{Nothing,String}=nothing`: Treatment name used when
  importing trial or environmental data.
- `entry_type::Union{Nothing,String}=nothing`: Entry type used when importing
  trial data.
- `population_type::Union{Nothing,String}=nothing`: Population type used when
  importing trial data.
- `relationship_type::Union{Nothing,String}=nothing`: Relationship type used
  when importing trial data.
- `measurement_dates::Union{Nothing,Dict{String,String}}=nothing`: Mapping of
  measurement names to dates used during tabular-data imports.
- `name::Union{Nothing,String}=nothing`: Name assigned to uploaded datasets
  such as reference genomes, VCFs, `Genomes`, `Phenomes`, and `Fit` objects.
- `note::Union{Nothing,String}=nothing`: Descriptive note associated with
  uploaded datasets.
- `fname_reference_genome::Union{Nothing,String}=nothing`: Path to a previously
  registered reference genome required when uploading VCF or `Genomes` files.
- `link_value_parser_traits::Union{Nothing,Function}=nothing`: Function used to
  extract trait names from the `trait` field of the `Phenomes` struct when
  populating the `phenomes_traits` relationship table.
- `link_value_parser_sites::Union{Nothing,Function}=nothing`: Function used to
  extract site names from the `trait` field of the `Phenomes` struct when
  populating the `phenomes_sites` relationship table.
- `link_value_parser_experiments::Union{Nothing,Function}=nothing`: Function
  used to extract experiment names from the `trait` field of the `Phenomes`
  struct when populating the `phenomes_experiments` relationship table.
- `link_value_parser_measurements::Union{Nothing,Function}=nothing`: Function
  used to extract measurement names from the `trait` field of the `Phenomes`
  struct when populating the `phenomes_measurements` relationship table.
- `link_value_parser_treatments::Union{Nothing,Function}=nothing`: Function
  used to extract treatment names from the `trait` field of the `Phenomes`
  struct when populating the `phenomes_treatments` relationship table.
- `verbose::Bool=false`: If `true`, display progress and status messages during
  file-type detection and upload.

# Returns

- `Nothing`: The detected dataset is uploaded to the database.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file type cannot be determined.
- `ErrorException`: If the file matches more than one supported format.
- Any exception raised by the underlying upload function.
- Any exception raised whilst connecting to the database.

# Notes

- File-type detection is performed before opening a database connection.
- Supported file types include:
  - Trial data (`Trials`)
  - Environmental data
  - Reference genome FASTA files
  - VCF files
  - `Genomes` JLD2 files
  - `Phenomes` JLD2 files
  - `Fit` JLD2 files
- Type detection is based on the same validation and parsing routines used by
  the corresponding upload functions.
- Exactly one file type must be identified. Ambiguous matches are treated as
  errors.
- All upload operations are delegated to specialised upload functions.
- A database connection is opened automatically and closed once the upload has
  completed.
- Optional arguments are forwarded only to upload functions that require them.
- When uploading `Phenomes` objects, the `link_value_parser_*` functions may be
  used to create relationships between phenotype traits and existing database
  records such as traits, sites, experiments, measurements, and treatments.
- Parser functions receive values from the `trait` field of the `Phenomes`
  struct and must return the corresponding database entity name to be linked.
- All generated database queries use validated and sanitised inputs through the
  package's filtering and upload infrastructure.
- This function provides a convenient high-level entry point for importing
  supported datasets without requiring the caller to determine the file type
  manually.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> # Trial data upload;

julia> fname = abspath(string("simulated_trials-", Dates.now(), ".tsv"));

julia> simulate_genomes() |> x -> simulate_trials(x, fname_trials_tsv = fname);

julia> df_trials_before = execute(conn, "SELECT * FROM phenotype_data") |> DataFrame;

julia> upload(fname, species="Zea mays", experiment=replace(replace(string(Dates.now()), ":"=>""), "."=>""), treatment="control", entry_type="family", population_type="cultivar", relationship_type="member_of");

julia> df_trials_after = execute(conn, "SELECT * FROM phenotype_data") |> DataFrame;

julia> nrow(df_trials_before) < nrow(df_trials_after)
true

julia> # Phenomes file upload;

julia> fname = abspath(string("simulated_phenomes-", Dates.now(), ".jld2"));

julia> simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2 = fname);

julia> df_phenomes_before = execute(conn, "SELECT * FROM phenomes") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data");

julia> df_phenomes_after = execute(conn, "SELECT * FROM phenomes") |> DataFrame;

julia> nrow(df_phenomes_before) < nrow(df_phenomes_after)
true

julia> # Environmental data upload;

julia> fname = abspath(string("simulated_environments-", Dates.now(), ".tsv"));

julia> simulate_genomes() |> simulate_trials |> x -> simulate_environments(x, fname_environments_tsv = fname);

julia> df_environments_before = execute(conn, "SELECT * FROM environment_data") |> DataFrame;

julia> upload(fname, experiment=replace(replace(string(Dates.now()), ":"=>""), "."=>""), treatment="control");

julia> df_environments_after = execute(conn, "SELECT * FROM environment_data") |> DataFrame;

julia> nrow(df_environments_before) < nrow(df_environments_after)
true

julia> # Upload reference genome;

julia> fname = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"));

julia> simulate_genomes(fname_reference_genome = fname);

julia> df_reference_genome_before = execute(conn, "SELECT * FROM reference_genomes") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data");

julia> df_reference_genome_after = execute(conn, "SELECT * FROM reference_genomes") |> DataFrame;

julia> nrow(df_reference_genome_before) < nrow(df_reference_genome_after)
true

julia> # Upload VCF;

julia> fname = abspath(string("simulated_genomes-", Dates.now(), ".vcf"));

julia> fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"));

julia> simulate_genomes(fname_genomes_vcf = fname, fname_reference_genome = fname_reference_genome);

julia> upload(fname_reference_genome, name=basename(fname_reference_genome), note="simulated data");

julia> df_vcf_before = execute(conn, "SELECT * FROM genotype_vcfs") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data", fname_reference_genome=fname_reference_genome);

julia> df_vcf_after = execute(conn, "SELECT * FROM genotype_vcfs") |> DataFrame;

julia> nrow(df_vcf_before) < nrow(df_vcf_after)
true

julia> # Upload Genomes;

julia> fname = abspath(string("simulated_genomes-", Dates.now(), ".jld2"));

julia> fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"));

julia> simulate_genomes(fname_genomes_jld2 = fname, fname_reference_genome = fname_reference_genome);

julia> upload(fname_reference_genome, name=basename(fname_reference_genome), note="simulated data");

julia> df_genomes_before = execute(conn, "SELECT * FROM genomes") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data", fname_reference_genome=fname_reference_genome);

julia> df_genomes_after = execute(conn, "SELECT * FROM genomes") |> DataFrame;

julia> nrow(df_genomes_before) < nrow(df_genomes_after)
true

julia> # Upload Fit;

julia> fname = abspath(string("simulated_fit-", Dates.now(), ".jld2"));

julia> genomes = simulate_genomes();

julia> phenomes = simulate_trials(genomes) |> simulate_phenomes;

julia> simulate_fit(genomes, phenomes, fname_fit_jld2 = fname);

julia> df_fit_before = execute(conn, "SELECT * FROM fits") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data");

julia> df_fit_after = execute(conn, "SELECT * FROM fits") |> DataFrame;

julia> nrow(df_fit_before) < nrow(df_fit_after)
true

julia> close(conn);
```
"""
function upload(
    fname::String;
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
    species::Union{Nothing,String} = nothing,
    experiment::Union{Nothing,String} = nothing,
    treatment::Union{Nothing,String} = nothing,
    entry_type::Union{Nothing,String} = nothing,
    population_type::Union{Nothing,String} = nothing,
    relationship_type::Union{Nothing,String} = nothing,
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing,
    name::Union{Nothing,String} = nothing,
    note::Union{Nothing,String} = nothing,
    fname_reference_genome::Union{Nothing,String} = nothing,
    link_value_parser_traits::Union{Nothing,Function} = nothing,
    link_value_parser_sites::Union{Nothing,Function} = nothing,
    link_value_parser_experiments::Union{Nothing,Function} = nothing,
    link_value_parser_measurements::Union{Nothing,Function} = nothing,
    link_value_parser_treatments::Union{Nothing,Function} = nothing,
    verbose::Bool = false,
)::Nothing
    # missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]
    # species::Union{Nothing,String} = nothing
    # experiment::Union{Nothing,String} = nothing
    # treatment::Union{Nothing,String} = nothing
    # entry_type::Union{Nothing,String} = nothing
    # population_type::Union{Nothing,String} = nothing
    # relationship_type::Union{Nothing,String} = nothing
    # measurement_dates::Union{Nothing,Dict{String,String}} = nothing
    # name::Union{Nothing,String} = nothing
    # note::Union{Nothing,String} = nothing
    # fname_reference_genome::Union{Nothing,String} = nothing
    # verbose::Bool = true
    # # Trial data upload
    # fname = abspath(string("simulated_trials-", Dates.now(), ".tsv"))
    # simulate_genomes() |> x -> simulate_trials(x, fname_trials_tsv = fname)
    # species::Union{Nothing,String} = "Zea mays"
    # experiment::Union{Nothing,String} = "sim-exp"
    # treatment::Union{Nothing,String} = "control"
    # entry_type::Union{Nothing,String} = "family"
    # population_type::Union{Nothing,String} = "cultivar"
    # relationship_type::Union{Nothing,String} = "member_of"
    # # Phenomes file upload
    # fname = abspath(string("simulated_phenomes-", Dates.now(), ".jld2"))
    # simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2 = fname)
    # name = isnothing(name) ? basename(fname) : name
    # note = isnothing(note) ? "simulated data" : note
    # # Environmental data upload
    # fname = abspath(string("simulated_environments-", Dates.now(), ".tsv"))
    # simulate_genomes() |> simulate_trials |> x -> simulate_environments(x, fname_environments_tsv = fname)
    # experiment::Union{Nothing,String} = "sim-exp"
    # treatment::Union{Nothing,String} = "control"
    # # Upload reference genome
    # fname = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_reference_genome = fname)
    # name = isnothing(name) ? basename(fname) : name
    # note = isnothing(note) ? "simulated data" : note
    # # Upload VCF
    # fname = abspath(string("simulated_genomes-", Dates.now(), ".vcf"))
    # fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_genomes_vcf = fname, fname_reference_genome = fname_reference_genome)
    # upload_reference_genome!(conn, fname = fname_reference_genome, name = isnothing(name) ? basename(fname_reference_genome) : name, note = isnothing(note) ? "simulated data" : note)
    # name = isnothing(name) ? basename(fname) : name
    # note = isnothing(note) ? "simulated data" : note
    # # Upload Genomes
    # fname = abspath(string("simulated_genomes-", Dates.now(), ".jld2"))
    # fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_genomes_jld2 = fname, fname_reference_genome = fname_reference_genome)
    # upload_reference_genome!(conn, fname = fname_reference_genome, name = isnothing(name) ? basename(fname_reference_genome) : name, note = isnothing(note) ? "simulated data" : note)
    # name = isnothing(name) ? basename(fname) : name
    # note = isnothing(note) ? "simulated data" : note
    # # Upload Fit
    # fname = abspath(string("simulated_fit-", Dates.now(), ".jld2"))
    # genomes = simulate_genomes()
    # phenomes = simulate_trials(genomes) |> simulate_phenomes
    # simulate_fit(genomes, phenomes, fname_fit_jld2 = fname)
    # name = isnothing(name) ? basename(fname) : name
    # note = isnothing(note) ? "simulated data" : note
    if !isfile(fname)
        error("The input file: \"$fname\" does not exist!")
    end
    # Determine input file type
    if verbose
        println("Determining data type of \"$fname\"...")
    end
    data_type_checks = Dict(
        # Phenotype data
        "trial_data" => try
            !isnothing(readdelimited(Trials, fname = fname))
        catch
            false
        end,
        "Phenomes" => try
            isnothing(check(Phenomes, fname = fname))
        catch
            false
        end,
        # Environmental data
        "environmental_data" => try
            !isnothing(extract_environment_variables(load_environments_df(fname, missing_strings = missing_strings)))
        catch
            false
        end,
        # Genptype data
        "reference_genome" => try
            isnothing(check_reference_genome(fname))
        catch
            false
        end,
        "vcf" => try
            isnothing(check_vcf(fname))
        catch
            false
        end,
        "Genomes" => try
            isnothing(check(Genomes, fname = fname))
        catch
            false
        end,
        # Model data
        "Fit" => try
            isnothing(check(Fit, fname = fname))
        catch
            false
        end,
    )
    filter!(x -> x.second, data_type_checks)
    if length(data_type_checks) == 0
        error("Unable to determine the type of \"$fname\"! Please refer to #link to file formats..(TODO...)")
    end
    if sum(values(data_type_checks)) > 1
        error(
            string(
                "Multiple format matches for \"$fname\"!\n\t- \"",
                join(keys(filter(x -> x.second, data_type_checks)), "\"\n\t- \""),
                "\"",
            ),
        )
    end
    # Upload
    conn = dbconnect()
    data_type = String.(keys(data_type_checks))[1]
    if verbose
        println("Uploading $data_type from \"$fname\"...")
    end
    if data_type == "trial_data"
        upload_trial_data!(
            conn,
            fname = fname,
            missing_strings = missing_strings,
            species = species,
            experiment = experiment,
            treatment = treatment,
            entry_type = entry_type,
            population_type = population_type,
            relationship_type = relationship_type,
            measurement_dates = measurement_dates,
            verbose = verbose,
        )
    elseif data_type == "Phenomes"
        upload_phenomes!(
            conn,
            fname = fname,
            name = name,
            note = note,
            link_value_parser_traits = link_value_parser_traits,
            link_value_parser_sites = link_value_parser_sites,
            link_value_parser_experiments = link_value_parser_experiments,
            link_value_parser_measurements = link_value_parser_measurements,
            link_value_parser_treatments = link_value_parser_treatments,
        )
    elseif data_type == "environmental_data"
        upload_environment_data!(
            conn,
            fname = fname,
            missing_strings = missing_strings,
            experiment = experiment,
            treatment = treatment,
            measurement_dates = measurement_dates,
            verbose = verbose,
        )
    elseif data_type == "reference_genome"
        upload_reference_genome!(conn, fname = fname, name = name, note = note)
    elseif data_type == "vcf"
        upload_genotype_vcf!(
            conn,
            fname = fname,
            name = name,
            note = note,
            fname_reference_genome = fname_reference_genome,
        )
    elseif data_type == "Genomes"
        upload_genomes!(conn; fname = fname, name = name, note = note, fname_reference_genome = fname_reference_genome)
    elseif data_type == "Fit"
        upload_fit!(conn, fname = fname, name = name, note = note)
    else
        error("Totally unexpected error as we expect the previous data type checks to catch all possible errors!")
    end
    close(conn)
    if verbose
        println("Success!")
    end
    nothing
end
