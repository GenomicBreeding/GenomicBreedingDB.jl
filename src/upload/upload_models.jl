"""
    upload_fit!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        note::String,
        fname_genomes::String,
        link_value_parser_traits::Function=x -> String(split(x, "|")[1]),
        link_value_parser_sites::Union{Nothing,Function}=nothing,
        link_value_parser_experiments::Union{Nothing,Function}=nothing,
        link_value_parser_measurements::Union{Nothing,Function}=nothing,
        link_value_parser_treatments::Union{Nothing,Function}=nothing,
        verbose::Bool=false,
    )::Nothing

Register a Fit file in the database and define relationships to associated
metadata and genomic resources.

The function inserts a Fit JLD2 file into the `fits` table and constructs
relationship records linking the fit to entries, traits, sites,
experiments, measurements, treatments, genomes, and reference genomes.

Relationship values are extracted from the Fit object and resolved against
metadata records already present in the database. Optional parser functions
may be supplied to extract metadata encoded within trait labels.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the Fit JLD2 file.
- `name::String`: Name assigned to the fit record.
- `note::String`: User-supplied description of the fit.
- `fname_genomes::String`: Absolute path to the Genomes file associated with
  the fit.
- `link_value_parser_traits::Function=x -> String(split(x, "|")[1])`:
  Function used to derive trait names from Fit trait labels.
- `link_value_parser_sites::Union{Nothing,Function}=nothing`: Function used to
  derive site names from Fit trait labels.
- `link_value_parser_experiments::Union{Nothing,Function}=nothing`: Function
  used to derive experiment names from Fit trait labels.
- `link_value_parser_measurements::Union{Nothing,Function}=nothing`: Function
  used to derive measurement names from Fit trait labels.
- `link_value_parser_treatments::Union{Nothing,Function}=nothing`: Function
  used to derive treatment names from Fit trait labels.
- `verbose::Bool=false`: If `true`, display progress information whilst
  constructing relationship records.

# Returns

- `Nothing`: The Fit record and all associated relationship records are
  inserted into the database.

# Throws

- `ErrorException`: If `fname` is not an absolute path.
- `ErrorException`: If `fname_genomes` is not an absolute path.
- `ErrorException`: If the Fit file fails validation.
- `ErrorException`: If the Genomes file fails validation.
- Any exception raised whilst inserting records into the database.
- Any exception raised whilst defining relationships.

# Warnings

- A warning is emitted if the Fit file has already been registered in the
  database.
- Relationship-definition functions may emit warnings when extracted metadata
  values cannot be matched to registered database records.

# Notes

- The Fit file is validated using `check(Fit, ...)`.
- The associated Genomes file is validated using `check(Genomes, ...)`.
- Both file paths must be supplied as absolute paths.
- Metadata are inserted into the `fits` table using:
  - `name`
  - `file_path`
  - `note`
- Duplicate registrations are ignored using
  `INSERT ... ON CONFLICT DO NOTHING`.
- Entry relationships are always created through:
  - `fits_entries`
- Trait relationships are always created through:
  - `fits_traits`
- Genome relationships are always created through:
  - `fits_genomes`
- Reference-genome relationships are always created through:
  - `fits_reference_genomes`
- Site relationships are optionally created through:
  - `fits_sites`
- Experiment relationships are optionally created through:
  - `fits_experiments`
- Measurement relationships are optionally created through:
  - `fits_measurements`
- Treatment relationships are optionally created through:
  - `fits_treatments`
- Relationship construction is delegated to
  `define_relationships!`.
- Existing relationship records are not duplicated.

## Trait Relationships

- Trait relationships are always defined.
- By default, trait names are derived using:

  ```julia
  x -> String(split(x, "|")[1])
  ```

- This assumes that the first pipe-delimited component of each trait label
  corresponds to a registered trait name.

For example:

```text
yield|experiment_01-site_a-control
```

is interpreted as:

```text
yield
```

when populating the `fits_traits` relationship table.

## Site, Experiment, Measurement, and Treatment Relationships

- Site relationships are created only when
  `link_value_parser_sites` is supplied.
- Experiment relationships are created only when
  `link_value_parser_experiments` is supplied.
- Measurement relationships are created only when
  `link_value_parser_measurements` is supplied.
- Treatment relationships are created only when
  `link_value_parser_treatments` is supplied.
- These parser functions allow metadata embedded within trait labels to be
  linked to existing database records.

For example:

```text
yield|experiment_01-site_a-control
```

may be parsed into:

```julia
x -> "experiment_01"
x -> "site_a"
x -> "control"
```

depending on the relationship being defined.

## Genome Relationships

- Genome relationships are always created through
  `fits_genomes`.
- The supplied `fname_genomes` file must already be registered in the
  database.
- Relationships are derived from the registered Genomes dataset rather than
  directly from the Fit object.

## Reference Genome Relationships

- Reference-genome relationships are always created through
  `fits_reference_genomes`.
- The associated reference genome is obtained from the registered Genomes
  dataset referenced by `fname_genomes`.
- The corresponding reference genome must already be registered in the
  database.

## Metadata Requirements

- Entries referenced by the Fit object should already exist in the database.
- Traits referenced by the Fit object should already exist in the database.
- Sites, experiments, measurements, and treatments must exist in their
  respective metadata tables before relationships can be created.
- Genomes and reference genomes associated with the fit must already be
  registered.
- Missing metadata values are not automatically registered.
- Unresolved values are reported by `define_relationships!` and may be added
  manually using helper functions such as `insert_names!(...)`.

# Relationships Created

The following relationship tables may be populated:

- `fits_entries` (always)
- `fits_traits` (always)
- `fits_genomes` (always)
- `fits_reference_genomes` (always)
- `fits_sites` (optional)
- `fits_experiments` (optional)
- `fits_measurements` (optional)
- `fits_treatments` (optional)

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_fit_jld2 = string("simulated_fit_jld2-", Dates.now(),".jld2");

julia> fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"));

julia> fname_genomes = abspath(string("simulated_genomes-", Dates.now(), ".jld2"));

julia> fname_phenomes = abspath(string("simulated_phenomes-", Dates.now(), ".jld2"));

julia> genomes = simulate_genomes(fname_genomes_jld2 = fname_genomes, fname_reference_genome = fname_reference_genome);

julia> phenomes = simulate_trials(genomes) |> x -> simulate_phenomes(x, fname_phenomes_jld2 = fname_phenomes);

julia> simulate_fit(genomes, phenomes, fname_fit_jld2 = fname_fit_jld2);

julia> upload(fname_reference_genome, name=basename(fname_reference_genome), note="simulated data");

julia> upload(fname_genomes, name=basename(fname_genomes), note="simulated data", fname_reference_genome=fname_reference_genome);

julia> upload(fname_phenomes, name=basename(fname_phenomes), note="simulated data");

julia> conn = dbconnect(); 

julia> upload_fit!(conn, fname=abspath(fname_fit_jld2), name=fname_fit_jld2, note="simulated", fname_genomes=fname_genomes);

julia> query(conn, [Filter(conn, table="fits", field="name", filter_in=[fname_fit_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_fit!(
    conn::LibPQ.Connection;
    fname::String,
    name::String,
    note::String,
    fname_genomes::String,
    link_value_parser_traits::Function = x -> String(split(x, "|")[1]),
    link_value_parser_sites::Union{Nothing,Function} = nothing,
    link_value_parser_experiments::Union{Nothing,Function} = nothing,
    link_value_parser_measurements::Union{Nothing,Function} = nothing,
    link_value_parser_treatments::Union{Nothing,Function} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect(); fname = abspath("simulated_fit.jld2"); genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> simulate_phenomes; simulate_fit(genomes, phenomes, fname_fit_jld2=fname); name = replace(fname, ".tsv" => ""); note = "simulated fit"; verbose=true
    # fname_genomes = abspath("simulated_genomes.jld2")
    # link_value_parser_traits = x -> String(split(x, '|')[1])
    # link_value_parser_sites = x -> String(split(split(x, '|')[2], "-")[end-1])
    # link_value_parser_experiments = x -> String("simulated experiment")
    # link_value_parser_measurements = x -> String(join(split(split(x, '|')[2], "-")[2:4], "-"))
    # link_value_parser_treatments = x -> String("control")
    check(Fit, fname = fname)
    check(Genomes, fname = fname_genomes)
    if !isabspath(fname)
        error("The path to the Fit file is not absolute: \"$fname\"!")
    end
    if !isabspath(fname_genomes)
        error("The path to the Genomes file is not absolute: \"$fname_genomes\"!")
    end
    res = execute(
        conn,
        """
        INSERT INTO fits
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
    define_relationships!(conn, table = "fits_entries", fname_jld2_or_vcf = fname, verbose = verbose)
    define_relationships!(
        conn,
        table = "fits_traits",
        fname_jld2_or_vcf = fname,
        link_value_parser = link_value_parser_traits,
        verbose = verbose,
    )
    if !isnothing(link_value_parser_sites)
        define_relationships!(
            conn,
            table = "fits_sites",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_sites,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_experiments)
        define_relationships!(
            conn,
            table = "fits_experiments",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_experiments,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_measurements)
        define_relationships!(
            conn,
            table = "fits_measurements",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_measurements,
            verbose = verbose,
        )
    end
    if !isnothing(link_value_parser_treatments)
        define_relationships!(
            conn,
            table = "fits_treatments",
            fname_jld2_or_vcf = fname,
            link_value_parser = link_value_parser_treatments,
            verbose = verbose,
        )
    end
    define_relationships!(
        conn,
        table = "fits_genomes",
        fname_jld2_or_vcf = fname,
        fname_genomes = fname_genomes,
        verbose = verbose,
    )
    define_relationships!(
        conn,
        table = "fits_reference_genomes",
        fname_jld2_or_vcf = fname,
        fname_genomes = fname_genomes,
        verbose = verbose,
    )
    # execute(conn, "SELECT * FROM fits") |> DataFrame
    # execute(conn, "SELECT * FROM fits_entries") |> DataFrame
    # execute(conn, "SELECT * FROM fits_traits") |> DataFrame
    # execute(conn, "SELECT * FROM fits_genomes") |> DataFrame
    # execute(conn, "SELECT * FROM fits_reference_genomes") |> DataFrame
    nothing
end
