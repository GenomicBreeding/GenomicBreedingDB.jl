"""
    define_relationships!(
        conn::LibPQ.Connection;
        table::String,
        fname_jld2_or_vcf::String,
        link_value_parser::Function=x -> x,
        fname_genomes::Union{Nothing,String}=nothing,
        verbose::Bool=false,
    )::Nothing

Populate a relationship table linking a registered file-based dataset to
existing metadata records in the database.

The function extracts names or identifiers from a registered Genomes,
Phenomes, Genotype VCF, or Fit file and inserts the corresponding records into
a relationship table. Extracted values are resolved against existing database
records and only valid relationships are inserted.

Relationship values may be transformed using a user-supplied parser function,
allowing compound labels to be mapped to traits, sites, experiments,
measurements, treatments, genomes, or reference genomes.

This function is primarily intended for use by dataset-upload helpers such as
`upload_genomes!`, `upload_genotype_vcf!`, `upload_phenomes!`, and
`upload_fit!`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Relationship table to populate.
- `fname_jld2_or_vcf::String`: Absolute path to a registered JLD2 or VCF file.
- `link_value_parser::Function=x -> x`: Function used to transform extracted
  metadata values into names recognised by the target metadata table.
- `fname_genomes::Union{Nothing,String}=nothing`: Absolute path to a related
  Genomes file. Required for:
  - `fits_genomes`
  - `fits_reference_genomes`
- `verbose::Bool=false`: If `true`, display insertion progress and summary
  information.

# Returns

- `Nothing`: Relationship records are inserted directly into the database.

# Throws

- `ErrorException`: If `table` is not a supported relationship table.
- `ErrorException`: If `fname_jld2_or_vcf` is not an absolute path.
- `ErrorException`: If the supplied dataset file fails validation.
- `ErrorException`: If the dataset file has not yet been registered in the
  database.
- `ErrorException`: If a genomes file is required but not supplied.
- `ErrorException`: If `fname_genomes` is not an absolute path.
- `ErrorException`: If the supplied genomes file has not yet been registered.
- Any exception raised during file parsing.
- Any exception raised during database insertion.

# Notes

- Relationship tables must already exist in the database schema.
- Supported relationship tables currently include:
  - `genomes_entries`
  - `genotype_vcfs_entries`
  - `phenomes_entries`
  - `phenomes_traits`
  - `phenomes_sites`
  - `phenomes_experiments`
  - `phenomes_measurements`
  - `phenomes_treatments`
  - `fits_entries`
  - `fits_traits`
  - `fits_sites`
  - `fits_experiments`
  - `fits_measurements`
  - `fits_treatments`
  - `fits_genomes`
  - `fits_reference_genomes`
- The source dataset type is inferred automatically from the relationship-table
  name.
- Supported source file types are:
  - `Genomes`
  - `Phenomes`
  - `Fit`
  - genotype VCF files
- Dataset files must already be registered in their corresponding database
  table before relationships can be defined.
- Relationship insertion is performed inside a database transaction.
- Any insertion failure causes a complete rollback.
- Duplicate relationships are ignored using
  `ON CONFLICT DO NOTHING`.
- Relationship values are resolved against the `name` field of the target
  metadata table.
- Values found in the source file but absent from the target metadata table are
  skipped and reported as warnings.
- Missing values are not automatically registered because they may indicate:
  - missing metadata registrations;
  - missing trial data;
  - inconsistent naming conventions;
  - incorrect parser logic.

## Metadata Extraction

- For Genomes, Phenomes, and Fit objects, metadata are extracted from the JLD2
  object using the appropriate object fields.
- For genotype VCF datasets, metadata are extracted directly from the VCF file.
- Extracted values are deduplicated before relationship insertion.
- The default parser is:

  ```julia
  x -> x
  ```

- Parser functions may be used to transform encoded metadata into valid
  database names.

## Entry Relationships

- `genomes_entries` relationships are derived from entries represented within
  the Genomes object.
- `genotype_vcfs_entries` relationships are derived from sample names stored
  within the VCF file.
- `phenomes_entries` relationships are derived from entries represented within
  the Phenomes object.
- `fits_entries` relationships are derived from entries used during model
  fitting.

## Trait Relationships

- `phenomes_traits` relationships are derived from trait labels stored within
  the Phenomes object.
- `fits_traits` relationships are derived from trait labels stored within the
  Fit object.

## Site, Experiment, Measurement, and Treatment Relationships

- `phenomes_sites`
- `phenomes_experiments`
- `phenomes_measurements`
- `phenomes_treatments`
- `fits_sites`
- `fits_experiments`
- `fits_measurements`
- `fits_treatments`

may be inferred from trait labels using `link_value_parser`.

For example, if a trait label contains embedded metadata:

```text
yield|experiment_01-site_a-control
```

then parser functions may extract:

```julia
x -> "yield"
x -> "experiment_01"
x -> "site_a"
x -> "control"
```

depending on the target relationship table.

## Fit-to-Genome Relationships

- `fits_genomes` relationships require the
  `fname_genomes` argument.
- The referenced Genomes file must already be registered in the database.
- Relationships are created using the registered genome dataset name rather
  than information stored directly within the Fit object.

## Fit-to-Reference-Genome Relationships

- `fits_reference_genomes` relationships require the
  `fname_genomes` argument.
- The referenced Genomes file must already be registered in the database.
- The associated reference genome is obtained from the registered Genomes
  record and linked to the Fit record.

## Verbose Output

When `verbose=true`, the function displays:

- relationship insertion progress;
- number of newly inserted relationships;
- number of existing relationships skipped;
- number of unresolved metadata values.

# Relationship Resolution Workflow

1. Validate inputs and table names.
2. Validate the supplied dataset file.
3. Confirm the dataset has already been registered.
4. Extract relationship values from the dataset.
5. Apply `link_value_parser`.
6. Resolve metadata names against registered database records.
7. Insert relationship records.
8. Commit the transaction.
9. Report any unresolved metadata values.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_jld2 = string("simulated_genotype_jld2-", Dates.now(),".jld2");

julia> fname_phenomes_jld2 = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> conn = dbconnect();

julia> n = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> simulate_genomes(n=maximum([100, n+1]), fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2) |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2);

julia> upload_trial_data!(conn, fname="simulated_trials.tsv", species="Acacia neglecta", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");

julia> upload_reference_genome!(conn, fname=abspath(fname_reference_genome), name=fname_reference_genome, note="simulated");

julia> upload_genomes!(conn, fname=abspath(fname_genomes_jld2), name=fname_genomes_jld2, note="simulated", fname_reference_genome=abspath(fname_reference_genome));

julia> link_value_parser_traits = x -> String(split(x, '|')[1]);

julia> link_value_parser_sites = x -> String(split(split(x, '|')[2], "-")[end-1]);

julia> link_value_parser_experiments = x -> String("simulated experiment");

julia> link_value_parser_measurements = x -> String(join(split(split(x, '|')[2], "-")[2:4], "-"));

julia> link_value_parser_treatments = x -> String("control");

julia> upload_phenomes!(conn, fname=abspath(fname_phenomes_jld2), name=fname_phenomes_jld2, note="simulated", link_value_parser_traits=link_value_parser_traits, link_value_parser_sites=link_value_parser_sites, link_value_parser_experiments=link_value_parser_experiments, link_value_parser_measurements=link_value_parser_measurements, link_value_parser_treatments=link_value_parser_treatments);

julia> n_before = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="genomes_entries", fname_jld2_or_vcf=abspath(fname_genomes_jld2));

julia> n_after = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_entries") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_entries", fname_jld2_or_vcf=abspath(fname_phenomes_jld2));

julia> n_after = execute(conn, "SELECT * FROM phenomes_entries") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_traits") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_traits", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser_traits);

julia> n_after = execute(conn, "SELECT * FROM phenomes_traits") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_sites", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser_sites);

julia> n_after = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_experiments") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_experiments", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser_experiments);

julia> n_after = execute(conn, "SELECT * FROM phenomes_experiments") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_measurements") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_measurements", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser_measurements);

julia> n_after = execute(conn, "SELECT * FROM phenomes_measurements") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_treatments") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_treatments", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser_treatments);

julia> n_after = execute(conn, "SELECT * FROM phenomes_treatments") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> close(conn);
```
"""
function define_relationships!(
    conn::LibPQ.Connection;
    table::String,
    fname_jld2_or_vcf::String,
    link_value_parser::Function = x -> x,
    fname_genomes::Union{Nothing,String} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # # table = "genomes_entries"; fname_jld2_or_vcf = abspath("simulated_genomes.jld2")
    # # table = "genotype_vcfs_entries"; fname_jld2_or_vcf = abspath("simulated_genomes.vcf")
    # # table = "phenomes_entries"; fname_jld2_or_vcf = abspath("simulated_phenomes.jld2")
    # # table = "phenomes_traits"; fname_jld2_or_vcf = abspath("simulated_phenomes.jld2")
    # # table = "fits_entries"; fname_jld2_or_vcf = abspath("simulated_fit.jld2")
    # # table = "fits_traits"; fname_jld2_or_vcf = abspath("simulated_fit.jld2")
    # # table = "fits_genomes"; fname_jld2_or_vcf = abspath("simulated_fit.jld2")
    # # table = "fits_reference_genomes"; fname_jld2_or_vcf = abspath("simulated_fit.jld2")
    # genomes = simulate_genomes()
    # phenomes = simulate_trials(genomes) |> simulate_phenomes
    # simulate_fit(genomes, phenomes)
    # upload_trial_data!(conn, fname=abspath("simulated_trials.tsv"), species="Zea mays", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");
    # upload_reference_genome!(conn, fname=abspath("simulated_reference_genome.fa"), name = "simulated", note = "simulated")
    # link_value_parser::Function = x -> String(split(x, '|')[1])
    # fname_genomes::Union{Nothing, String} = abspath("simulated_genomes.jld2")
    # verbose = true
    check(conn)
    check_illegal_strings([table])
    if !isabspath(fname_jld2_or_vcf)
        error("The path to the file is not absolute: \"$fname_jld2_or_vcf\"!")
    end
    valid_table_names =
        list_all_tables(conn) |>
        df ->
            filter!(x -> !isnothing(match(Regex("^genomes_|^phenomes_|^genotype_vcfs_|^fits_"), x.table_name)), df) |>
            df -> df.table_name
    if table∉valid_table_names
        error("Invalid table: \"$table\"!")
    end
    vec_splits = String.(split(table, "_"))
    table_1, table_2 = if (vec_splits[1] == "reference") || (vec_splits[1] == "genotype")
        join(vec_splits[1:(end-1)], "_"), vec_splits[end]
    else
        vec_splits[1], join(vec_splits[2:end], "_")
    end
    type = if table_1 == "genomes"
        Genomes
    elseif table_1 == "phenomes"
        Phenomes
    elseif table_1 == "genotype_vcfs"
        "VCF"
    elseif table_1 == "fits"
        Fit
    else
        error("Invalid table_1: \"$table_1\"!")
    end
    id_1 = replace(table_1, Regex("s\$") => "_id")
    id_2 = table_2 == "entries" ? "entry_id" : replace(table_2, Regex("s\$") => "_id")
    check_illegal_strings([id_1, id_2])
    if type != "VCF"
        check(type, fname = fname_jld2_or_vcf)
    end
    df_record_1 = query(conn, [Filter(conn, table = table_1, field = "file_path", filter_in = [fname_jld2_or_vcf])])
    if nrow(df_record_1) == 0
        throw(
            string(
                "The $type file \"$fname_jld2_or_vcf\" is not found in the database. ",
                "Please check the path or use `upload_$(table_1)!(...)` first!",
            ),
        )
    end
    link_values = if type != Fit
        # `link_values` at the moment does not affect the Genomes-related relationship table
        field = table_2 != "entries" ? Symbol("traits") : Symbol(table_2)
        X = if type != "VCF"
            readjld2(type, fname = fname_jld2_or_vcf)
        else
            readvcf(fname = fname_jld2_or_vcf)
        end
        link_values = unique(vcat(getproperty(X, field)))
        link_value_parser.(link_values)
    else
        # Fit struct where we expect `fname_genomes`
        link_values = if (table_2 == "genomes") || (table_2 == "reference_genomes")
            if isnothing(fname_genomes)
                error("For the \"$table relationship\" table, we expect the \"fname_genomes\" argument to be defined!")
            end
            if !isabspath(fname_genomes)
                error("We expect the absolute path to the related genomes file: \"$fname_genomes\"!")
            end
            df_tmp =
                query(conn, [Filter(conn, table = "genomes", field = "file_path", filter_in = [fname_genomes])])
            if nrow(df_tmp) == 0
                error(
                    "The Genomes file has not yet been registered in the database! Please consider `upload(\"$fname_genomes\")`",
                )
            end
            if table_2 == "genomes"
                df_tmp.name
            else
                df_tmp.reference_genome
            end
        else
            field = table_2 != "entries" ? :trait : :entries
            X = if type != "VCF"
                readjld2(type, fname = fname_jld2_or_vcf)
            else
                readvcf(fname = fname_jld2_or_vcf)
            end
            link_values = unique(vcat(getproperty(X, field)))
            link_value_parser.(link_values)
        end
    end
    unregistered_node_2 = String[]
    n_new = 0
    n_old = 0
    execute(conn, "BEGIN")
    try
        pb = ProgressMeter.Progress(length(link_values), desc = "Inserting records into \"$table\" table...")
        for x in link_values
            # x = link_values[1]
            df_record_2 = query(conn, [Filter(conn, table = table_2, field = "name", filter_in = [x])])
            if nrow(df_record_2) == 0
                push!(unregistered_node_2, x)
                continue
            end
            res = execute(
                conn,
                """
                INSERT INTO $table
                ($id_1, $id_2)
                VALUES (\$1, \$2)
                ON CONFLICT DO NOTHING
                """,
                vcat(df_record_1.id, df_record_2.id),
            )
            if LibPQ.num_affected_rows(res) > 0
                n_new += 1
            else
                n_old += 1
            end
            if verbose
                ProgressMeter.next!(pb)
            end
        end
        execute(conn, "COMMIT")
        if verbose
            ProgressMeter.next!(pb)
        end
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    unique!(unregistered_node_2)
    if length(unregistered_node_2) > 0
        @warn join([
            "The following values were found in the $type file but are absent in the database. ",
            "This can mean that they do not have any associated trial/phenotype data ",
            "or the default `link_value_parser` is incorrect (link_value_parser::Function = x -> String(split(x, '|')[1])). ",
            "These have not been automatically registered by this function, you can manually register them via `insert_names!(...)`:\n\t- \"",
            join(unregistered_node_2, "\"\n\t- \""),
            "\"",
        ])
    end
    if verbose
        println(
            string(
                "Inserted $n_new new records into the \"$table\" table. ",
                "Skipped $n_old existing records and ",
                length(unregistered_node_2),
                " unregistered_node_2 entries.",
            ),
        )
    end
    # # TODO: Updload the other relationship tables stemming from entry and trait names...
    # # e.g. yet to be defined tables: phenomes_species, genomes_species, etc...
    # # On second thought, since we are already filtering by entry names and entry names are unique across species, then this may just not be needed.
    # link_values
    # id_1
    # if id_2 == "entry_id"
    #     df_entries = query(conn, [Filter(conn, table = "entries", field = "name", filter_in = link_values)])
    #     species_names = string.(unique(df_entries.species))
    #     entry_type_names = string.(unique(df_entries.entry_type))
    #     species_ids = query(conn, [Filter(conn, table = "species", field = "name", filter_in = species_names)]).id
    #     entry_type_ids = query(conn, [Filter(conn, table = "entry_types", field = "name", filter_in = entry_type_names)]).id
    # ...
    # execute(conn, "SELECT * FROM $table") |> DataFrame
    nothing
end
