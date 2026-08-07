"""
    define_relationships!(
        conn::LibPQ.Connection;
        table::String,
        fname_jld2_or_vcf::String,
        link_value_parser::Function=x -> String(split(x, '|')[1]),
        verbose::Bool=false,
    )::Nothing

Populate a database relationship table using values extracted from a registered
genotype or phenotype dataset.

The function creates relationships between a registered dataset and existing
database entities by extracting values from a JLD2 or VCF file and inserting the
corresponding links into a relationship table.

Supported relationship tables include those linking `genomes`, `genotype_vcfs`,
and `phenomes` datasets to entries, traits, sites, experiments, measurements,
and treatments. The function identifies the relevant dataset record, extracts
the linked values from the source file, resolves them to database identifiers,
and inserts the resulting relationships whilst avoiding duplicate records.

Relationship creation is performed inside a database transaction to ensure
consistency and atomicity.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Relationship table to populate.
- `fname_jld2_or_vcf::String`: Path to a registered JLD2 or VCF file.
- `link_value_parser::Function=x -> String(split(x, '|')[1])`: Function used to
  transform extracted values into entity names recognised by the database.
- `verbose::Bool=false`: If `true`, display progress information and summary
  statistics during relationship creation.

# Returns

- `Nothing`: Relationship records are inserted into the specified relationship
  table.

# Throws

- `ErrorException`: If the specified relationship table is invalid.
- `ErrorException`: If the dataset file is not registered in the database.
- `ErrorException`: If the dataset file fails validation.
- Any database exception raised whilst querying or inserting records.
- Any exception raised by the supplied `link_value_parser`.
- Any exception raised whilst reading the source file.

# Warnings

- A warning is emitted when values extracted from the source file cannot be
  matched to records already present in the corresponding metadata table.
- Unmatched values are skipped and relationship records are not created for
  them.
- The warning may indicate missing metadata records or an incorrect
  `link_value_parser`.

# Notes

- Supported relationship tables include all tables whose names begin with:
    + `genomes_`
    + `genotype_vcfs_`
    + `phenomes_`
- The relationship type is inferred automatically from `table`.
- Supported source datasets include:
    +  `Genomes`
    +  `Phenomes`
    +  VCF files
- Dataset validation is performed before any relationship records are created.
- The supplied dataset must already be registered in the database.
- The source dataset record is located using its absolute file path.
- Relationships are inserted using `INSERT ... ON CONFLICT DO NOTHING`, making
  repeated executions safe.
- Relationship creation is performed inside an explicit database transaction.
- If an error occurs during insertion, all changes are rolled back.
- Entry relationships are derived from the dataset `entries` field.
- All other relationships are currently derived from the dataset `traits`
  field.
- The `link_value_parser` receives values extracted from the source dataset and
  must return entity names corresponding to records already present in the
  associated metadata table.
- For phenotype datasets, the parser can be used to extract traits, sites,
  experiments, measurements, or treatments encoded within trait names,
  which may be encoded as "yield_kg_per_ha-highN-Hamilton-2026_Early_Spring".
- Duplicate extracted values are removed before relationship creation.
- Existing relationship records are detected automatically and are not
  duplicated.
- Progress information is displayed when `verbose=true`.
- Summary statistics report:
    + newly inserted relationships,
    + existing relationships skipped, and
    + unmatched values encountered during processing.

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

julia> define_relationships!(conn, table="phenomes_traits", fname_jld2_or_vcf=abspath(fname_phenomes_jld2));

julia> n_after = execute(conn, "SELECT * FROM phenomes_traits") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> link_value_parser = x -> String(split(split(x, '|')[2], "-")[end-1]);

julia> define_relationships!(conn, table="phenomes_sites", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser);

julia> n_after = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_experiments") |> DataFrame |> nrow;

julia> link_value_parser = x -> String("simulated experiment");

julia> define_relationships!(conn, table="phenomes_experiments", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser);

julia> n_after = execute(conn, "SELECT * FROM phenomes_experiments") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_measurements") |> DataFrame |> nrow;

julia> link_value_parser = x -> String(join(split(split(x, '|')[2], "-")[2:4], "-"));

julia> define_relationships!(conn, table="phenomes_measurements", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser);

julia> n_after = execute(conn, "SELECT * FROM phenomes_measurements") |> DataFrame |> nrow;

julia> n_before <= n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_treatments") |> DataFrame |> nrow;

julia> link_value_parser = x -> String("control");

julia> define_relationships!(conn, table="phenomes_treatments", fname_jld2_or_vcf=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser);

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
    link_value_parser::Function = x -> String(split(x, '|')[1]),
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # # table = "genomes_entries"; fname_jld2_or_vcf = "simulated_genomes.jld2"
    # # table = "genotype_vcfs_entries"; fname_jld2_or_vcf = "simulated_genomes.vcf"
    # # table = "phenomes_entries"; fname_jld2_or_vcf = "simulated_phenomes.jld2"
    # # table = "phenomes_traits"; fname_jld2_or_vcf = "simulated_phenomes.jld2"
    # simulate_genomes() |> simulate_trials |> simulate_phenomes
    # upload_trial_data!(conn, fname=abspath("simulated_trials.tsv"), species="Zea mays", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");
    # upload_reference_genome!(conn, fname=abspath("simulated_reference_genome.fa"), name = "simulated", note = "simulated")
    # link_value_parser::Function = x -> String(split(x, '|')[1])
    # verbose = true
    check(conn)
    check_illegal_strings([table])
    valid_table_names =
        list_all_tables(conn) |>
        df ->
            filter!(x -> !isnothing(match(Regex("^genomes_|^phenomes_|^genotype_vcfs_"), x.table_name)), df) |>
            df -> df.table_name
    if table∉valid_table_names
        error("Invalid table: \"$table\"!")
    end
    table_1 = join(String.(split(table, "_"))[1:(end-1)], "_")
    table_2 = String.(split(table, "_"))[end]
    type = if table_1 == "genomes"
        Genomes
    elseif table_1 == "phenomes"
        Phenomes
    elseif table_1 == "genotype_vcfs"
        "VCF"
    else
        error("Invalid table: \"$table\"!")
    end
    id_1 = replace(table_1, Regex("s\$") => "_id")
    id_2 = table_2 == "entries" ? "entry_id" : replace(table_2, Regex("s\$") => "_id")
    check_illegal_strings([id_1, id_2])
    if type != "VCF"
        check(type, fname = fname_jld2_or_vcf)
    end
    df_record_1 =
        query(conn, [Filter(conn, table = table_1, field = "file_path", filter_in = [abspath(fname_jld2_or_vcf)])])
    if nrow(df_record_1) == 0
        throw(
            string(
                "The $type file \"$fname_jld2_or_vcf\" is not found in the database. ",
                "Please check the path or use `upload_$(table_1)!(...)` first!",
            ),
        )
    end
    link_values = let
        # `link_values` at the moment does the affect the Genomes-related relationship table
        field = table_2 != "entries" ? Symbol("traits") : Symbol(table_2)
        X = if type != "VCF"
            readjld2(type, fname = fname_jld2_or_vcf)
        else
            readvcf(fname = fname_jld2_or_vcf)
        end
        link_values = unique(getproperty(X, field))
        link_value_parser.(link_values)
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
