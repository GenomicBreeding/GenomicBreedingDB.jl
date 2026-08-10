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
        fname_genomes::Union{Nothing,String}=nothing,
        fname_reference_genome::Union{Nothing,String}=nothing,
        link_value_parser_traits::Union{Nothing,Function}=nothing,
        link_value_parser_sites::Union{Nothing,Function}=nothing,
        link_value_parser_experiments::Union{Nothing,Function}=nothing,
        link_value_parser_measurements::Union{Nothing,Function}=nothing,
        link_value_parser_treatments::Union{Nothing,Function}=nothing,
        verbose::Bool=false,
    )::Nothing

Automatically detect the type of an input file and upload its contents into
the database.

The function serves as the primary high-level upload interface for all
supported database resources. It inspects the supplied file, determines its
data type, validates the content, and dispatches the file to the appropriate
specialised upload function.

Supported uploads include trial data, environmental data, reference genomes,
genotype VCFs, Genomes objects, Phenomes objects, and Fit objects.

# Arguments

- `fname::String`: Path to the file to upload.
- `missing_strings::Vector{String}`: Strings interpreted as missing values
  during tabular-data imports.
- `species::Union{Nothing,String}=nothing`: Species name associated with trial
  data uploads.
- `experiment::Union{Nothing,String}=nothing`: Experiment name associated with
  trial or environmental data uploads.
- `treatment::Union{Nothing,String}=nothing`: Treatment name associated with
  trial or environmental data uploads.
- `entry_type::Union{Nothing,String}=nothing`: Entry type associated with trial
  data uploads.
- `population_type::Union{Nothing,String}=nothing`: Population type associated
  with trial data uploads.
- `relationship_type::Union{Nothing,String}=nothing`: Relationship type
  associated with trial data uploads.
- `measurement_dates::Union{Nothing,Dict{String,String}}=nothing`: Optional
  measurement-date mapping for trial and environmental data uploads.
- `name::Union{Nothing,String}=nothing`: Name assigned to uploaded file-based
  datasets.
- `note::Union{Nothing,String}=nothing`: User-supplied description of the
  uploaded dataset.
- `fname_genomes::Union{Nothing,String}=nothing`: Associated Genomes file
  required when uploading Fit objects.
- `fname_reference_genome::Union{Nothing,String}=nothing`: Reference genome
  required when uploading Genomes or genotype VCF datasets.
- `link_value_parser_traits::Union{Nothing,Function}=nothing`: Trait parser
  used when uploading Phenomes or Fit datasets.
- `link_value_parser_sites::Union{Nothing,Function}=nothing`: Site parser used
  when uploading Phenomes or Fit datasets.
- `link_value_parser_experiments::Union{Nothing,Function}=nothing`:
  Experiment parser used when uploading Phenomes or Fit datasets.
- `link_value_parser_measurements::Union{Nothing,Function}=nothing`:
  Measurement parser used when uploading Phenomes or Fit datasets.
- `link_value_parser_treatments::Union{Nothing,Function}=nothing`: Treatment
  parser used when uploading Phenomes or Fit datasets.
- `verbose::Bool=false`: If `true`, display progress and status messages.

# Returns

- `Nothing`: Records are inserted directly into the database.

# Throws

- `ErrorException`: If the supplied file does not exist.
- `ErrorException`: If the file format cannot be determined.
- `ErrorException`: If multiple file-format checks succeed.
- `ErrorException`: If uploading a Fit object without `fname_genomes`.
- Any exception raised by the delegated upload functions.

# Notes

- The input file must exist before upload can proceed.
- File type detection is performed automatically.
- Exactly one supported file type must match.
- The database connection is opened automatically and closed upon completion.
- Upload logic is delegated to specialised upload functions.
- The function prints progress messages when `verbose=true`.

## Supported File Types

The following file types are supported:

- Trial data (`upload_trial_data!`)
- Environmental data (`upload_environment_data!`)
- Reference genomes (`upload_reference_genome!`)
- Genotype VCFs (`upload_genotype_vcf!`)
- Genomes JLD2 files (`upload_genomes!`)
- Phenomes JLD2 files (`upload_phenomes!`)
- Fit JLD2 files (`upload_fit!`)

## Trial Data Uploads

When the detected file contains trial data, the following arguments may be
used:

- `species`
- `experiment`
- `treatment`
- `entry_type`
- `population_type`
- `relationship_type`
- `measurement_dates`
- `missing_strings`

These arguments are ignored for all other file types.

## Environmental Data Uploads

When the detected file contains environmental data, the following arguments
may be used:

- `experiment`
- `treatment`
- `measurement_dates`
- `missing_strings`

All other upload-specific arguments are ignored.

## Reference Genome Uploads

When uploading a reference genome:

- `name` should identify the reference assembly.
- `note` may be used to describe the assembly source or version.

All trial- and phenotype-related arguments are ignored.

## Genotype VCF Uploads

When uploading a genotype VCF:

- `name` identifies the VCF dataset.
- `note` describes the dataset.
- `fname_reference_genome` should point to an already registered reference
  genome.

All trial-data arguments are ignored.

## Genomes Uploads

When uploading a Genomes object:

- `name` identifies the dataset.
- `note` describes the dataset.
- `fname_reference_genome` should identify the associated reference genome.

All trial-data arguments are ignored.

## Phenomes Uploads

When uploading a Phenomes object:

- `name` identifies the dataset.
- `note` describes the dataset.
- `link_value_parser_traits`
- `link_value_parser_sites`
- `link_value_parser_experiments`
- `link_value_parser_measurements`
- `link_value_parser_treatments`

may be used to define metadata relationships from trait labels.

All trial-specific arguments are ignored.

## Fit Uploads

When uploading a Fit object:

- `fname_genomes` is required.
- `name` identifies the fit.
- `note` describes the fit.
- `link_value_parser_traits`
- `link_value_parser_sites`
- `link_value_parser_experiments`
- `link_value_parser_measurements`
- `link_value_parser_treatments`

may be used to define metadata relationships.

All trial-specific arguments are ignored.

## File-Based Dataset Uploads

The following file-based dataset types support metadata relationships:

- `Phenomes`
- `Genomes`
- `GenotypeVCFs`
- `Fits`

For these dataset types:

- trial-layout information is not uploaded;
- plot-level observations are not uploaded;
- `species`, `entry_type`, `population_type`, and
  `relationship_type` are ignored;
- relationships are inferred from metadata already stored within the file
  itself and from records already registered in the database.

## Upload Dispatch

Detected file types are mapped to upload functions as follows:

- `trial_data` → `upload_trial_data!`
- `environmental_data` → `upload_environment_data!`
- `reference_genome` → `upload_reference_genome!`
- `vcf` → `upload_genotype_vcf!`
- `Genomes` → `upload_genomes!`
- `Phenomes` → `upload_phenomes!`
- `Fit` → `upload_fit!`

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

julia> fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"));

julia> fname_genomes = abspath(string("simulated_genomes-", Dates.now(), ".jld2"));

julia> fname_phenomes = abspath(string("simulated_phenomes-", Dates.now(), ".jld2"));

julia> genomes = simulate_genomes(fname_genomes_jld2 = fname_genomes, fname_reference_genome = fname_reference_genome);

julia> phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2 = fname_phenomes);

julia> simulate_fit(genomes, phenomes, fname_fit_jld2 = fname);

julia> upload(fname_reference_genome, name=basename(fname_reference_genome), note="simulated data");

julia> upload(fname_genomes, name=basename(fname_genomes), note="simulated data", fname_reference_genome=fname_reference_genome);

julia> upload(fname_phenomes, name=basename(fname_phenomes), note="simulated data");

julia> df_fit_before = execute(conn, "SELECT * FROM fits") |> DataFrame;

julia> upload(fname, name=basename(fname), note="simulated data", fname_genomes=fname_genomes);

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
    fname_genomes::Union{Nothing,String} = nothing,
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
    # fname_genomes::Union{Nothing,String} = nothing
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
            verbose = verbose,
        )
    elseif data_type == "Genomes"
        upload_genomes!(conn; fname = fname, name = name, note = note, fname_reference_genome = fname_reference_genome)
    elseif data_type == "Fit"
        if isnothing(fname_genomes)
            error("Uploading Fit data requires \"fname_genomes\"!")
        end
        upload_fit!(conn, fname = fname, fname_genomes = fname_genomes, name = name, note = note, verbose = verbose)
    else
        error("Totally unexpected error as we expect the previous data type checks to catch all possible errors!")
    end
    close(conn)
    if verbose
        println("Success!")
    end
    nothing
end
