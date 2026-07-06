
# INPUT FILES
# -----------
# trial_data.tsv ← main dataset
# phenomes.tsv   ← phenome metadata
# genomes.tsv    ← genotype metadata
# fits.tsv       ← model metadata
# ↓
# JULIA PIPELINE
# --------------
# parse
# normalise
# map names → IDs
# insert
# ↓
# POSTGRES DB
# -----------
# fully normalised schema

function simulate(; 
    output_fname::String = "simulated_trial_data.tsv", 
    additional_params::Union{Nothing, Dict{String, String}} = nothing,
    overwrite::Bool = true,
    verbose::Bool = false,
)::String
    # output_fname::String = "simulated_trial_data.tsv"; overwrite::Bool = true; verbose::Bool = false; additional_params::Union{Nothing, Dict{String, String}} = nothing
    # additional_params::Union{Nothing, Dict{String, String}} = Dict("species" => "Lolium multiflorum", "experiment" => "STR_trial-2026", "treatment" => "control")
    genomes = GenomicBreedingCore.simulategenomes(verbose=verbose)
    (trials, _) = GenomicBreedingCore.simulatetrials(genomes=genomes, verbose=verbose)
    if overwrite && isfile(output_fname)
        rm(output_fname)
    end
    if isnothing(additional_params)
        GenomicBreedingIO.writedelimited(trials, fname=output_fname)
    else
        df = tabularise(trials)
        for (k, v) in additional_params
            df[!, k] .= v
        end
        CSV.write(output_fname, df; delim='\t')
    end
    output_fname
end

function validate_trials(df::DataFrame)::Nothing
    required_columns = sort(filter(x -> isnothing(match(Regex("phenotypes|traits"), x)), String.(string.(collect(fieldnames(Trials))))))
    if required_columns != sort(required_columns ∩ names(df))
        throw("Missing columns: [\"$(join(setdiff(required_columns, names(df)), "\", \""))\"] in \"$fname\".")
    end
    nothing
end

function add_col!(df::DataFrame; col::String, value::Union{Nothing, String})::Nothing
    # df = CSV.read(simulate(), DataFrame); col = "species"; value = nothing
    # df = CSV.read(simulate(), DataFrame); col = "species"; value = "Lolium multiflorum"
    if col ∈ names(df)
        if !isnothing(value)
            @warn("Using \"$col\" col in the dataframe instead of the supplied \"$col = $value\".")
        end
    else
        if isnothing(value)
            throw("Please define the \"$col\" of the entries in the dataframe as no \"$col\" col was detected.")
        end
        df[!, col] .= value
    end
    nothing
end

function validate_date(date::String)::Bool
    date_split = split(date, '/')
    # date_split = split(date_split, '-') # we are being very strict here, i.e. we assume yyyy/mm/dd date format!
    if (
        (length(date_split) != 3) || 
        (length(date_split[1]) != 4) || 
        ((length(date_split[2]) < 1) && (length(date_split[2]) > 2)) || 
        ((length(date_split[3]) < 1) && (length(date_split[3]) > 2)) ||
        sum(isnothing.(tryparse.(Int64, date_split))) > 0
    )
        false
    else
        true
    end
end

function layout_info_parser!(df::DataFrame)::Nothing
    validate_trials(df)
    for f in [:replications, :blocks, :rows, :cols]
        # f = :replications
        isa(df[!, f], Vector{Int64}) ? continue : nothing
        df[!, f] = try
            df[!, f] |>
                x -> [split(xi, "_")[end] for xi in x] |>
                x -> [split(xi, "-")[end] for xi in x] |>
                x -> [split(xi, "|")[end] for xi in x] |>
                x -> [parse(Int64, xi) for xi in x]
        catch
            throw("Cannot parse $(f)!")
        end
    end
end

function add_measurement_dates!(df::DataFrame; measurement_dates::Union{Nothing, Dict{String, String}})::Nothing
    # df = CSV.read(simulate(), DataFrame); measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # df = CSV.read(simulate(), DataFrame); df[!, "dates"] = String.(df.measurements); measurement_dates::Union{Nothing, Dict{String, String}} = nothing
    # df = CSV.read(simulate(), DataFrame); measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in unique(df.measurements)]
    if "dates" ∈ names(df)
        if !isnothing(measurement_dates)
            @warn("Using the \"dates\" col in the dataframe.")
        end
        dates = unique(df.dates) # dates[1] = "2025/JA/01"
        if !isa(dates, Vector{DateTime}) && (sum(.!validate_date.(dates)) > 0)
            throw("Invalid date format/s: [\"$(join(dates, "\", \""))\"]. We expect \"yyyy/mm/dd\" format, where all values are integers.")
        end
    else
        if isnothing(measurement_dates)
            throw("Please supply the measurement dates either as \"dates\" in the dataframe or as a dictionary mapping the \"measurements\" with \"dates\". Format of dates: 'yyyy/mm/dd'.")
        end
        measurements = sort(String.(unique(df.measurements)))
        measurements_input = sort(String.(keys(measurement_dates)))
        if measurements != sort(measurements ∩ measurements_input)
            throw("Please define all the dates for all the measurements. We have the following measurements: [$(join(measurements, ", "))] but only the following were defined in the input: [$(join(measurements_input, ", "))]")
        end
        df[!, "dates"] .= Dates.now()
        for (k, v) in measurement_dates
            # k = string.(keys(measurement_dates))[1]; v = measurement_dates[k]
            # v = "10062026"
            # v = "2025-03-dd"
            if !validate_date(v)
                throw("Invalid date format: \"$v\". We expect \"yyyy/mm/dd\" format, where all values are integers.")
            end
            idx = findall(df.measurements .== k)
            # println("k=$k; v=$v; length(idx)=$(length(idx))")
            length(idx) == 0 ? throw("Measurement \"$k\" not found in the dataframe!") : nothing
            df.dates[idx] .= Date(v, dateformat"yyyy/mm/dd")
        end
    end
    nothing
end

function insert_names!(
    conn::LibPQ.Connection;
    df::DataFrame, 
    table::String, 
    df_col::String,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = CSV.read(simulate(), DataFrame)
    # table = "entries"
    # df_col = "entries"
    # verbose::Bool = true
    if df_col ∉ names(df)
        throw("The \"$df_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!")
    end
    uploaded_names = select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
    existing_names = let
        df_tmp = try
            DataFrame(execute(conn,"SELECT name FROM $table;"))
        catch
            throw("Missing \"$table\" table in the database! (Note that the existence of the 'name' field is checked every time a connection to the database is made via `dbconnect()`.)")
        end
        String.(string.(df_tmp[:, 1]))
    end
    counter = 0
    pb = ProgressMeter.Progress(length(uploaded_names), "Inserting names listed in \"$df_col\" into \"$table\" table...")
    for x in uploaded_names
        # x = uploaded_names[1]
        if x ∉ existing_names
            execute(
                conn,
                """
                INSERT INTO $table (name)
                VALUES (\$1);
                """,
                [x]
            )
            counter += 1
            verbose ? ProgressMeter.next!(pb) : nothing
        end
    end
    if verbose
        ProgressMeter.finish!(pb)
        println("Inserted $counter new names in the \"$table\" table.")
    end
    nothing
end

function update_table_field_by_name!(
    conn::LibPQ.Connection;
    df::DataFrame, 
    table::String, 
    df_name_col::String,
    df_source_col::String,
    table_destination_field::String,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = CSV.read(simulate(), DataFrame); add_measurement_dates!(df, measurement_dates=measurement_dates)
    # table = "measurements"
    # df_name_col = "measurements"
    # df_source_col = "dates"
    # table_destination_field = "measure_date"
    # verbose::Bool = true
    if df_name_col ∉ names(df)
        throw("The \"$df_name_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!")
    end
    if df_source_col ∉ names(df)
        throw("The \"$df_source_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!")
    end
    table_exists = nrow(DataFrame(execute(conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = \$1
        """,
        [table]
    ))) > 0
    if !table_exists
        throw("The \"$table\" table does not exist in the database!")
    end
    field_exists = nrow(DataFrame(execute(conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = \$1
        AND column_name = \$2
        """,
        [table, table_destination_field]
    ))) > 0
    if !field_exists
        throw("The \"$table_destination_field\" field does not exist in the \"$table\" table!")
    end
    # We extract the ids if we need to update the ids from some related table, i.e. if we have the pattern "*_id" for the `table_destination_field`
    df_tmp = if split(table_destination_field, "_")[end] == "id"
        df_tmp = unique(select(df, [df_name_col, df_source_col]))
        root_table = join(split(table_destination_field, "_")[1:(end-1)], "_")
        ids = String[]
        for x in df_tmp[!, df_source_col]
            # x = df_tmp[!, df_source_col][1]
            push!(ids, execute(conn, "SELECT id FROM $root_table WHERE name = \$1", [x]) |> DataFrame |> x -> first(x.id))
        end
        df_tmp[!, df_source_col] = ids
        df_tmp
    else
        unique(select(df, [df_name_col, df_source_col]))
    end
    pb = ProgressMeter.Progress(nrow(df_tmp), desc="Updating $(nrow(df_tmp)) values of the \"$table_destination_field\" field in the \"$table\" table at ...")
    for i in 1:nrow(df_tmp)
        # i = 1
        execute(
            conn,
            """
            UPDATE $table
            SET
                $table_destination_field = \$1,
                updated_at = now()
            WHERE name = \$2
            ;
            """,
            [df_tmp[i, df_source_col], df_tmp[i, df_name_col]]
        )
        verbose ? ProgressMeter.next!(pb) : nothing
    end
    verbose ? ProgressMeter.finish!(pb) : nothing
    nothing
end

function insert_entry_relationships!(conn::LibPQ.Connection; df::DataFrame)::Nothing
    expected_columns = ["entries", "populations", "relationship_types"]
    if sum([x ∉ names(df) for x in expected_columns]) > 0
        throw("We have missing columns: [\", $(join(setdiff(expected_columns, names(df)), "\", \""))\"]")
    end
    entry_population_relationship = string.(df.entries, "|||", df.populations, "|||", df.relationship_types) |> 
        unique |>
        x -> split.(x, "|||")
    for i in eachindex(entry_population_relationship)
        # i = 1
        child = entry_population_relationship[i][1]
        parent = entry_population_relationship[i][2]
        rel_type = entry_population_relationship[i][3]
        child_id = execute(conn, "SELECT id FROM entries WHERE name = \$1", [child]) |> DataFrame |> x -> first(x.id)
        parent_id = execute(conn, "SELECT id FROM entries WHERE name = \$1", [parent]) |> DataFrame |> x -> first(x.id)
        execute(
            conn,
            """
            INSERT INTO entry_relationships
            (
                child_id,
                parent_id,
                rel_type
            )
            VALUES (\$1, \$2, \$3)
            ON CONFLICT (child_id, parent_id, rel_type) DO NOTHING
            """,
            [child_id, parent_id, rel_type]
        )
    end
    nothing
end

function load_trial_data(
    conn::LibPQ.Connection,
    fname::String;
    species::Union{Nothing, String} = nothing,
    experiment::Union{Nothing, String} = nothing,
    treatment::Union{Nothing, String} = nothing,
    measurement_dates::Union{Nothing, Dict{String, String}} = nothing,
    entry_type::Union{Nothing, String} = nothing,
    population_type::Union{Nothing, String} = nothing,
    relationship_type::Union{Nothing, String} = nothing,
    verbose::Bool = true,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # fname = simulate()
    # species::String = "Lolium multiflorum"
    # experiment::String = "STR_trial-2026"
    # treatment::String = "control"; verbose::Bool = true
    # measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); df = CSV.read(fname, DataFrame); [measurement_dates[x] = x for x in unique(df.measurements)]
    # entry_type::Union{Nothing, String} = "family"
    # population_type::Union{Nothing, String} = "population"
    # relationship_type::Union{Nothing, String} = "parent_is"
    df = CSV.read(fname, DataFrame)
    try
        rename!(df, "#years" => "years")
    catch
        nothing
    end
    validate_trials(df)
    layout_info_parser!(df)
    add_col!(df, col="species", value=species)
    add_col!(df, col="experiments", value=experiment)
    add_col!(df, col="treatments", value=treatment)
    add_col!(df, col="entry_types", value=entry_type)
    add_col!(df, col="population_types", value=population_type)
    add_col!(df, col="relationship_types", value=relationship_type)
    add_measurement_dates!(df; measurement_dates=measurement_dates)
    df[!, "years_seasons"] = string.("seasons=", df.years, "|", df.seasons, ";")
    df[!, "layouts"] .= string.("replication=", df.replications, "|block", df.blocks, "|row", df.rows, "|col", df.cols)

    insert_names!(conn, df=df, table="species", df_col="species", verbose=verbose)
    insert_names!(conn, df=df, table="experiments", df_col="experiments", verbose=verbose)
    insert_names!(conn, df=df, table="treatments", df_col="treatments", verbose=verbose)
    insert_names!(conn, df=df, table="measurements", df_col="measurements", verbose=verbose)
    insert_names!(conn, df=df, table="layouts", df_col="layouts", verbose=verbose)
    insert_names!(conn, df=df, table="entries", df_col="entries", verbose=verbose)
    insert_names!(conn, df=df, table="entries", df_col="populations", verbose=verbose)
    # insert_names!(conn, df=df, table="entries", df_col="layouts", verbose=verbose)
    # delete_names!(conn, df=df, table="entries", df_col="layouts", verbose=verbose)
    # execute(conn, "SELECT * FROM entries") |> DataFrame

    update_table_field_by_name!(conn, df=df, table="measurements", df_name_col="measurements", df_source_col="dates", table_destination_field="measure_date", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="measurements", df_name_col="measurements", df_source_col="years_seasons", table_destination_field="notes", verbose=verbose)
    # df_tmp = execute(conn, "SELECT * FROM measurements") |> DataFrame |> x -> select(x, [:name])
    # delete_names!(conn, df=df_tmp, table="measurements", df_col="name", verbose=verbose)
    # execute(conn, "SELECT * FROM measurements") |> DataFrame

    update_table_field_by_name!(conn, df=df, table="layouts", df_name_col="layouts", df_source_col="replications", table_destination_field="replication", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="layouts", df_name_col="layouts", df_source_col="blocks", table_destination_field="block", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="layouts", df_name_col="layouts", df_source_col="rows", table_destination_field="row", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="layouts", df_name_col="layouts", df_source_col="cols", table_destination_field="col", verbose=verbose)
    # execute(conn, "SELECT * FROM layouts") |> DataFrame

    update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="entry_types", table_destination_field="entry_type", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="entries", df_name_col="populations", df_source_col="population_types", table_destination_field="entry_type", verbose=verbose)
    update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="species", table_destination_field="species_id", verbose=verbose)
    # execute(conn, "SELECT * FROM entries") |> DataFrame

    insert_entry_relationships!(conn, df=df)
    # execute(conn, "SELECT * FROM entry_relationships") |> DataFrame
    # ids_parents = execute(conn, "SELECT * FROM entry_relationships") |> DataFrame |> x -> unique(x.parent_id)
    # execute(conn, "SELECT * FROM entries WHERE id IN (\$1)", [join(ids_parents, ",")]) |> DataFrame

    # TODO: extract traits
    # TODO: extract phenotype data and connect with the other related tables...






    
    # Extract, define (new) and update (existing):
    #   - field ==> "name"
    #       + species
    #       + entries
    #       + experiments
    #       + sites
    #       + treatments
    #   - TODO: species, experiments, 
    #   - TODO: measurements requires dates
    #       + measurements
    #   - TODO: field => "name" but traits are column names
    #       + traits
    #   - TODO: fields ==> ["replication", "block", "row", "col"]
    #       + layouts
    #   - TODO: more complex stuff...
    #       + entry_relationships
    #       + phenotype_data

    tables = ["species", "entries", "experiments", "sites", "treatments"]
    











    # # Ensure base entities
    # ensure_species = ensure_values(conn, "species", "name")
    # ensure_sites = ensure_values(conn, "sites", "name")
    # ensure_treatments = ensure_values(conn, "treatments", "name")
    # ensure_traits = ensure_values(conn, "traits", "name")

    # ensure_species(df.species)
    # ensure_sites(df.site)
    # ensure_treatments(df.treatment)
    # ensure_traits(names(df)[14:end])  # trait columns

    # # experiments
    # for yr in unique(df.year)
    #     execute(conn, """
    #     INSERT INTO experiments (name, planting_date)
    #     VALUES (\$1, \$2)
    #     ON CONFLICT DO NOTHING
    #     """, ("trial_$yr", Date(yr,5,1)))
    # end

    # # measurements
    # for m in unique(df.measurement)
    #     execute(conn, """
    #     INSERT INTO measurements (name, measure_date)
    #     VALUES (\$1, \$2)
    #     ON CONFLICT DO NOTHING
    #     """, (m, Date(2025,10,1)))
    # end

    # # layouts
    # for r in unique(df.row)
    #     execute(conn, """
    #     INSERT INTO layouts (replication, block, row, col)
    #     VALUES (\$1,\$2,\$3,\$4)
    #     ON CONFLICT DO NOTHING
    #     """, (1,1,r,1))
    # end

    # # -----------------------------
    # # Entries + populations
    # # -----------------------------
    # species_map = load_map(conn, "species", "name")

    # for r in eachrow(df)

    #     # population
    #     execute(conn, """
    #     INSERT INTO entries (name, species, entry_type)
    #     VALUES (\$1,\$2,'population')
    #     ON CONFLICT DO NOTHING
    #     """, (r.population, species_map[r.species]))

    #     # individual
    #     execute(conn, """
    #     INSERT INTO entries (name, species, entry_type)
    #     VALUES (\$1,\$2,'individual')
    #     ON CONFLICT DO NOTHING
    #     """, (r.entry, species_map[r.species]))

    # end

    # entry_map = load_map(conn, "entries", "name")

    # # relationships
    # for r in eachrow(df)
    #     execute(conn, """
    #     INSERT INTO entry_relationships (child_id, parent_id, rel_type)
    #     VALUES (\$1,\$2,'member_of')
    #     ON CONFLICT DO NOTHING
    #     """, (
    #         entry_map[r.entry],
    #         entry_map[r.population]
    #     ))
    # end

    # # -----------------------------
    # # Phenotype data
    # # -----------------------------
    # trait_cols = names(df)[14:end]

    # long = stack(df, trait_cols, variable_name=:trait, value_name=:value)

    # trait_map = load_map(conn, "traits", "name")
    # site_map = load_map(conn, "sites", "name")
    # treatment_map = load_map(conn, "treatments", "name")
    # exp_map = load_map(conn, "experiments", "name")

    # meas_id = only(execute(conn, "SELECT id FROM measurements LIMIT 1") |> DataFrame).id

    # for r in eachrow(long)

    #     layout_id = only(execute(conn,
    #         "SELECT id FROM layouts WHERE row=\$1",
    #         (r.row,)
    #     ) |> DataFrame).id

    #     execute(conn, """
    #     INSERT INTO phenotype_data (
    #         experiment_id, site_id, treatment_id, layout_id,
    #         measurement_id, trait_id, entry_id, value
    #     ) VALUES (\$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8)
    #     """, (
    #         exp_map["trial_$(r.year)"],
    #         site_map[r.site],
    #         treatment_map[r.treatment],
    #         layout_id,
    #         meas_id,
    #         trait_map[string(r.trait)],
    #         entry_map[r.entry],
    #         r.value
    #     ))
    # end

    nothing
end

# function load_phenomes(conn, file)

#     df = CSV.read(file, DataFrame)

#     entry_map = load_map(conn, "entries", "name")
#     trait_map = load_map(conn, "traits", "name")
#     exp_map = load_map(conn, "experiments", "name")

#     for r in eachrow(df)

#         execute(conn, "INSERT INTO phenomes (file_path) VALUES (\$1)", (r.file_path,))

#         ph_id = only(execute(conn,
#             "SELECT id FROM phenomes WHERE file_path=\$1",
#             (r.file_path,)
#         ) |> DataFrame).id

#         for e in split(r.entries, ';')
#             execute(conn,
#                 "INSERT INTO phenome_entries VALUES (\$1,\$2)",
#                 (ph_id, entry_map[e]))
#         end

#         for t in split(r.traits, ';')
#             execute(conn,
#                 "INSERT INTO phenome_traits VALUES (\$1,\$2)",
#                 (ph_id, trait_map[t]))
#         end

#     end
# end