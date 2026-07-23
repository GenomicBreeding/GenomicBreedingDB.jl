"""
    define_relationships!(
        conn::LibPQ.Connection;
        table::String,
        fname_jld2::String,
        link_value_parser::Function=x -> String(split(x, '|')[1]),
        verbose::Bool=false,
    )::Nothing

Create relationship records between a registered GenomicBreeding dataset and a
database entity table.

The function populates a junction table linking a registered `Genomes` or
`Phenomes` dataset to records stored in another database table, such as
`entries`, `traits`, `sites`, `treatments`, or other supported entities. The
dataset type is inferred from the relationship table name and the corresponding
JLD2 file is loaded to extract relationship values.

Values extracted from the dataset are optionally transformed using
`link_value_parser`, matched against records in the corresponding database table,
and inserted into the specified relationship table. Existing relationships are
preserved through the use of `ON CONFLICT DO NOTHING`.

For `Genomes` datasets, relationship values are extracted directly from the
property corresponding to the target table, i.e. "entries". For `Phenomes` 
datasets, values are extracted from the `traits` field and may be parsed into 
other entities using `link_value_parser`. This allows relationship tables 
such as `phenomes_sites` and `phenomes_treatments` to be populated when trait 
names follow a consistent naming convention containing embedded metadata 
separated by delimiters (for example `"Trait|Treatment-Site-Year"`). In these 
cases, `link_value_parser` can be used to extract the relevant component prior to
database matching.

All insert operations are performed within a single transaction. If an error
occurs during processing, the transaction is rolled back and the exception is
re-raised.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the relationship table to populate. Must correspond to
  a valid table matching the pattern `genomes_*` or `phenomes_*`.
- `fname_jld2::String`: Path to a registered JLD2 file containing a `Genomes` or
  `Phenomes` object.
- `link_value_parser::Function=x -> String(split(x, '|')[1])`: Function used to
  transform extracted values before matching them against database records.
- `verbose::Bool=false`: If `true`, display progress information and summary
  statistics during processing.

# Returns

- `Nothing`: Relationship records are inserted into the specified relationship
  table.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If `table` is not a valid relationship table.
- `ErrorException`: If the supplied JLD2 file does not exist.
- `ErrorException`: If the supplied file does not appear to contain the expected
  object type.
- `ErrorException`: If the associated dataset has not been registered in the
  database.
- Any database exception raised during processing is rethrown after transaction
  rollback.
- Any exception raised while loading the JLD2 dataset.

# Warnings

- A warning is emitted when values extracted from the dataset cannot be found in
  the corresponding database table.
- Unmatched values are skipped and no relationship records are created for them.
- An incorrect `link_value_parser` may result in otherwise valid relationships
  being omitted.

# Notes

- Connection validation is performed using `check(conn)`.
- Valid relationship tables are discovered automatically from existing database
  tables whose names match `genomes_*` or `phenomes_*`.
- The dataset type is inferred from the relationship table prefix:
  - `genomes_*` → `Genomes`
  - `phenomes_*` → `Phenomes`
- File validation is performed using `check(type; fname=fname_jld2)`.
- The dataset must already be registered in the corresponding table (`genomes`
  or `phenomes`).
- Dataset records are identified using the stored absolute file path.
- For `Genomes`, relationship values are extracted directly from the target
  property.
- For `Phenomes`, relationship values are extracted from the `traits` field and
  transformed using `link_value_parser`.
- This design allows a single `Phenomes` dataset to populate multiple
  relationship tables (e.g. `phenomes_traits`, `phenomes_sites`,
  `phenomes_treatments`) when trait names encode multiple pieces of metadata in
  a consistent format.
- Duplicate values are removed prior to processing.
- Relationship records are inserted using `ON CONFLICT DO NOTHING`.
- All database modifications occur within a transaction using `BEGIN`,
  `COMMIT`, and `ROLLBACK`.
- When `verbose=true`, progress information and summary statistics describing
  inserted, skipped, and unmatched records are displayed.
- This function provides a generic mechanism for populating relationship tables
  involving `Genomes` and `Phenomes` datasets.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_jld2 = string("simulated_genotype_jld2-", Dates.now(),".jld2");

julia> fname_phenomes_jld2 = string("simulated_phenotype_jld2-", Dates.now(),".jld2");

julia> conn = dbconnect();

julia> n = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> simulate_genomes(n=n+1, fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2) |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2=fname_phenomes_jld2);

julia> upload_trial_data!(conn, fname="simulated_trials.tsv", species="Acacia neglecta", experiment="some-exp", treatment="some_trt", entry_type="family", population_type="population", relationship_type="member_of");

julia> upload_reference_genome!(conn, fname=abspath(fname_reference_genome), name=fname_reference_genome, notes="simulated");

julia> upload_genomes!(conn, fname=abspath(fname_genomes_jld2), name=fname_genomes_jld2, notes="simulated", fname_reference_genome=abspath(fname_reference_genome));

julia> upload_phenomes!(conn, fname=abspath(fname_phenomes_jld2), name=fname_phenomes_jld2, notes="simulated");

julia> n_before = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="genomes_entries", fname_jld2=abspath(fname_genomes_jld2));

julia> n_after = execute(conn, "SELECT * FROM genomes_entries") |> DataFrame |> nrow;

julia> n_before < n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_traits") |> DataFrame |> nrow;

julia> define_relationships!(conn, table="phenomes_traits", fname_jld2=abspath(fname_phenomes_jld2));

julia> n_after = execute(conn, "SELECT * FROM phenomes_traits") |> DataFrame |> nrow;

julia> n_before < n_after
true

julia> n_before = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> link_value_parser = x -> String(split(split(x, '|')[2], "-")[end-1]);

julia> define_relationships!(conn, table="phenomes_sites", fname_jld2=abspath(fname_phenomes_jld2), link_value_parser=link_value_parser);

julia> n_after = execute(conn, "SELECT * FROM phenomes_sites") |> DataFrame |> nrow;

julia> n_before < n_after
true

julia> close(conn);
```
"""
function define_relationships!(
    conn::LibPQ.Connection;
    table::String,
    fname_jld2::String,
    link_value_parser::Function = x -> String(split(x, '|')[1]),
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # # table = "genomes_entries"; fname_jld2 = "simulated_genomes.jld2"
    # # table = "phenomes_entries"; fname_jld2 = "simulated_phenomes.jld2"
    # table = "phenomes_traits"; fname_jld2 = "simulated_phenomes.jld2"
    # simulate_genomes() |> simulate_trials |> simulate_phenomes
    # upload_reference_genome!(conn, fname=abspath("simulated_reference_genome.fa"), name = "simulated", notes = "simulated")
    # upload_genomes!(conn, fname = abspath("simulated_genomes.jld2"), name = "simulated", notes = "simulated", fname_reference_genome = abspath("simulated_reference_genome.fa"))
    # upload_phenomes!(conn, fname = abspath("simulated_phenomes.jld2"), name = "simulated", notes = "simulated")
    # link_value_parser::Function = x -> String(split(x, '|')[1])
    # verbose = true
    check(conn)
    valid_table_names =
        extract_all_tables(conn) |>
        df ->
            filter!(x -> !isnothing(match(Regex("^genomes_|^phenomes_"), x.table_name)), df) |>
            df -> filter!(x -> length(split(x.table_name, "_")) == 2, df) |> df -> df.table_name
    if table∉valid_table_names
        error("Invalid table: \"$table\"!")
    end
    table_1, table_2 = String.(split(table, "_"))
    type = if table_1 == "genomes"
        Genomes
    elseif table_1 == "phenomes"
        Phenomes
    else
        error("Invalid table: \"$table\"!")
    end
    id_1 = replace(table_1, Regex("s\$") => "_id")
    id_2 = table_2 == "entries" ? "entry_id" : replace(table_2, Regex("s\$") => "_id")
    check(type, fname = fname_jld2)
    df_record_1 = query_table(
        conn,
        filters = [Filter(conn, table = table_1, field = "file_path", filter_in = [abspath(fname_jld2)])],
    )
    if nrow(df_record_1) == 0
        throw(
            string(
                "The $type file \"$fname_jld2\" is not found in the database. ",
                "Please check the path or use `upload_$(table_1)!(...)` first!",
            ),
        )
    end
    link_values = let
        field = table_1 == "phenomes" ? Symbol("traits") : Symbol(table_2)
        readjld2(type, fname = fname_jld2) |> x -> unique(getproperty(x, field)) |> x -> link_value_parser.(x)
    end
    unregistered_node_2 = String[]
    n_new = 0
    n_old = 0
    execute(conn, "BEGIN")
    try
        pb = ProgressMeter.Progress(length(link_values), "Inserting records into \"$table\" table...")
        for x in link_values
            # x = link_values[1]
            df_record_2 = query_table(conn, filters = [Filter(conn, table = table_2, field = "name", filter_in = [x])])
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
    # execute(conn, "SELECT * FROM $table") |> DataFrame
    nothing
end
