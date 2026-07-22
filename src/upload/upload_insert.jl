"""
    insert_names!(
        conn::LibPQ.Connection;
        df::DataFrame,
        table::String,
        df_col::String,
        verbose::Bool=false,
    )::Nothing

Insert unique names from a DataFrame column into a database table.

The function extracts unique values from the specified DataFrame column and inserts
them into the `name` field of the target database table. Existing names are ignored
using an `ON CONFLICT DO NOTHING` clause, allowing the operation to be safely
repeated without creating duplicate records.

All inserts are performed within a database transaction. If an error occurs during
insertion, the transaction is rolled back and the original exception is rethrown.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing names to be inserted.
- `table::String`: Name of the target database table.
- `df_col::String`: Name of the DataFrame column containing the names to insert.
- `verbose::Bool=false`: If `true`, display progress information and a summary of
  inserted records.

# Returns

- `Nothing`: Names are inserted into the database and the input DataFrame is not
  modified.

# Throws

- `ErrorException`: If the specified table does not exist.
- `ErrorException`: If the table does not contain a `name` column.
- `ErrorException`: If `df_col` is not present in the DataFrame.
- `ErrorException`: If one or more values in `df_col` contain illegal strings.
- Any database exception raised during insertion is rethrown after the transaction
  is rolled back.

# Notes

- The target table must contain a unique constraint on the `name` column for
  conflict handling to work correctly.
- Values are converted to strings, sorted, and deduplicated before insertion.
- Inserts are wrapped in a single transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Existing records are preserved through the use of
  `ON CONFLICT (name) DO NOTHING`.
- Progress reporting is available when `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_insert_names-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> conn = dbconnect();

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> close(conn);

julia> nrow(df_before) < nrow(df_after)
true
```
"""
function insert_names!(
    conn::LibPQ.Connection;
    df::DataFrame,
    table::String,
    df_col::String,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = CSV.read(simulate_trial(), DataFrame)
    # table = "entries"
    # df_col = "entries"
    # verbose::Bool = true
    check(conn, table)
    check(conn, table, "name")
    if df_col∉names(df)
        error(
            "The \"$df_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    try
        check_illegal_strings(String.(unique(df[!, df_col])))
    catch e
        new_error = join(["Illegal string in the \"$df_col\" column!\n", sprint(showerror, e)])
        error(new_error)
    end
    uploaded_names = select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
    counter = 0
    pb =
        ProgressMeter.Progress(length(uploaded_names), "Inserting names listed in \"$df_col\" into \"$table\" table...")
    execute(conn, "BEGIN")
    try
        for x in uploaded_names
            # x = uploaded_names[1]
            res = execute(
                conn,
                """
                INSERT INTO $table (name)
                VALUES (\$1)
                ON CONFLICT (name) DO NOTHING
                """,
                [x],
            )
            if LibPQ.num_affected_rows(res) > 0
                counter += 1
            end
            verbose ? ProgressMeter.next!(pb) : nothing
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Inserted $counter new names in the \"$table\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end

"""
    insert_layouts!(
        conn::LibPQ.Connection;
        df::DataFrame,
        is_trial::Bool=true,
        verbose::Bool=false,
    )::Nothing

Insert unique layout definitions from a DataFrame into the `layouts` database
table.

The function parses layout information from the input DataFrame, extracts unique
layout combinations, and inserts them into the `layouts` table. Each layout is
represented by a composite name of the form
`"<replication>-<block>-<row>-<col>"` together with its individual component
values.

All insert operations are performed within a single database transaction. Existing
layouts are ignored using an `ON CONFLICT DO NOTHING` clause, ensuring that repeated
imports do not generate duplicate records.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing layout information.
- `is_trial::Bool=true`: If `true`, validate and parse the DataFrame as trial
  data before extracting layouts.
- `verbose::Bool=false`: If `true`, display progress information and summary
  messages during insertion.

# Returns

- `Nothing`: Layout information is inserted into the database and the input
  DataFrame may be modified in place during parsing.

# Throws

- `ErrorException`: If the `layouts` table does not exist.
- `ErrorException`: If layout information cannot be parsed from the DataFrame.
- Any database exception raised during insertion is rethrown after the transaction
  is rolled back.

# Notes

- Layout identifiers are generated and standardised using `parse_layouts!`.
- Each unique layout is inserted only once.
- Insert operations are wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Existing records are preserved through the use of
  `ON CONFLICT (name) DO NOTHING`.
- Progress reporting is available when `verbose=true`.
- The function performs database writes but does not return the inserted records.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> conn = dbconnect();

julia> insert_layouts!(conn, df=df);

julia> n = execute(conn, "SELECT * FROM layouts") |> DataFrame |> nrow;

julia> m = string.(df.replications, "-", df.blocks, "-", df.rows, "-", df.cols) |> unique |> length;

julia> n >= m
true
```
"""
function insert_layouts!(conn::LibPQ.Connection; df::DataFrame, is_trial::Bool = true, verbose::Bool = false)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = load_trial_df(simulate_trial())
    # verbose::Bool = true
    check(conn, "layouts")
    parse_layouts!(df, is_trial = is_trial)
    ids = split.(unique(df.layouts), "-")
    execute(conn, "BEGIN")
    counter = 0
    pb = ProgressMeter.Progress(length(ids), "Inserting layout information...")
    try
        for i = 1:length(ids)
            # i = 1
            name = join(ids[i], "-")
            replication = ids[i][1]
            block = ids[i][2]
            row = ids[i][3]
            col = ids[i][4]
            execute(
                conn,
                """
                INSERT INTO layouts
                (
                    name, 
                    replication, 
                    block, 
                    row, 
                    col
                ) 
                VALUES (\$1, \$2, \$3, \$4, \$5)
                ON CONFLICT (name) DO NOTHING
                """,
                [name, replication, block, row, col],
            )
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Inserted $counter relationships between entries in the \"$table\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end

"""
    insert_entry_relationships!(
        conn::LibPQ.Connection;
        df::DataFrame,
        verbose::Bool=false,
    )::Nothing

Insert relationships between entries and populations into the
`entry_relationships` database table.

The function extracts unique combinations of entries, populations, and relationship
types from the supplied DataFrame and inserts them into the database. Entry and
population names are first resolved to their corresponding identifiers in the
`entries` table before relationship records are created.

All insert operations are performed within a single transaction. Existing
relationships are ignored using an `ON CONFLICT DO NOTHING` clause, allowing the
function to be safely re-run without creating duplicate records.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing relationship information.
- `verbose::Bool=false`: If `true`, display progress information and summary
  messages during insertion.

# Returns

- `Nothing`: Relationship records are inserted into the database.

# Throws

- `ErrorException`: If the `entry_relationships` table does not exist.
- `ErrorException`: If any required columns are missing from the DataFrame.
- `ErrorException`: If an invalid relationship type is encountered.
- Any database exception raised during insertion is rethrown after the transaction
  is rolled back.

# Notes

- The input DataFrame must contain the columns `entries`, `populations`, and
  `relationship_types`.
- Supported relationship types are:
  `member_of`, `clone_of`, `parent_is`, `maternal_parent_is`, and
  `paternal_parent_is`.
- Entry and population names are resolved to identifiers using lookups against the
  `entries` table.
- Insert operations are wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Existing records are preserved through the use of
  `ON CONFLICT (child_id, parent_id, rel_type) DO NOTHING`.
- Progress reporting is available when `verbose=true`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> df.entries = string.("test_entry_rels-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> df.populations = string.("test_entry_rels-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.populations);

julia> df[!, "relationship_types"] .= "member_of";

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries");

julia> insert_names!(conn, df=df, table="entries", df_col="populations");

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entry_relationships") |> DataFrame;

julia> insert_entry_relationships!(conn, df=df);

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entry_relationships") |> DataFrame;

julia> close(conn);

julia> nrow(df_before) < nrow(df_after)
true
```
"""
function insert_entry_relationships!(conn::LibPQ.Connection; df::DataFrame, verbose::Bool = false)::Nothing
    check(conn, "entry_relationships")
    expected_columns = ["entries", "populations", "relationship_types"]
    if sum([x∉names(df) for x in expected_columns]) > 0
        missing_columns = setdiff(expected_columns, names(df))
        if "relationship_types" ∈ missing_columns
            error(
                "We have missing columns: [\"$(join(missing_columns, "\", \""))\"]. (Hint: relationship_types: {\"member_of\", \"clone_of\", \"parent_is\", \"maternal_parent_is\", \"paternal_parent_is\"})",
            )
        else
            error("We have missing columns: [\"$(join(missing_columns, "\", \""))\"]")
        end
    end
    entry_population_relationship =
        string.(df.entries, "|||", df.populations, "|||", df.relationship_types) |> unique |> x -> split.(x, "|||")
    counter = 0
    pb = ProgressMeter.Progress(
        length(entry_population_relationship),
        desc = "Inserting relationships between entries and populations...",
    )
    execute(conn, "BEGIN")
    try
        for i in eachindex(entry_population_relationship)
            # i = 1
            child = entry_population_relationship[i][1]
            parent = entry_population_relationship[i][2]
            rel_type = entry_population_relationship[i][3]
            if rel_type∉["member_of", "clone_of", "parent_is", "maternal_parent_is", "paternal_parent_is"]
                error("Invalide relationship type: \"$rel_type\".")
            end
            child_id =
                execute(conn, "SELECT id FROM entries WHERE name = \$1", [child]) |> DataFrame |> x -> first(x.id)
            parent_id =
                execute(conn, "SELECT id FROM entries WHERE name = \$1", [parent]) |> DataFrame |> x -> first(x.id)
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
                [child_id, parent_id, rel_type],
            )
            counter += 1
            verbose ? ProgressMeter.next!(pb) : nothing
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Inserted $counter relationships between entries in the \"entry_relationships\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end

"""
    insert_phenotype_data!(
        conn::LibPQ.Connection;
        df::DataFrame,
        traits::Vector{String},
        verbose::Bool=false,
    )::Nothing

Insert phenotype observations from a DataFrame into the `phenotype_data` database
table.

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
    insert_environment_data!(
        conn::LibPQ.Connection;
        df::DataFrame,
        environment_variables::Vector{String},
        verbose::Bool=false,
    )::Nothing

Insert environmental observations from a DataFrame into the `environment_data`
database table.

The function resolves identifiers for experiments, sites, treatments, layouts,
measurements, and environmental variables before importing environmental
observations into the database. For each row in the input DataFrame and each
specified environmental variable, a record is created linking the relevant
experimental factors to the observed value.

All insert operations are performed within a single transaction. Existing records
are ignored using an `ON CONFLICT DO NOTHING` clause, allowing repeated imports
without creating duplicate observations.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `df::DataFrame`: DataFrame containing environmental observations and associated
  metadata.
- `environment_variables::Vector{String}`: Names of environmental variable columns
  to import.
- `verbose::Bool=false`: If `true`, display progress information during import.

# Returns

- `Nothing`: Environmental records are inserted into the database.

# Throws

- `ErrorException`: If the `environment_data` table does not exist.
- `ErrorException`: If any required reference table has not been initialised.
- Any database exception raised during insertion is rethrown after the transaction
  is rolled back.

# Notes

- The following reference tables must be populated before import:
  `experiments`, `sites`, `treatments`, `layouts`, `measurements`, and
  `environment_variables`.
- Identifier values are resolved using `extract_ids` prior to data insertion.
- Missing environmental values are stored as `NaN`.
- One database record is generated for each combination of DataFrame row and
  environmental variable.
- Insert operations are wrapped in a transaction using `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Existing records are preserved through the use of a composite
  `ON CONFLICT DO NOTHING` constraint.
- Progress reporting is available when `verbose=true`.
- The function assumes all environmental variable names have already been
  registered in the `environment_variables` table.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials |> simulate_environments;

julia> df = load_environments_df("simulated_environments.tsv");

julia> conn = dbconnect();

julia> add_col!(df, col = "replications", value = "1"); add_col!(df, col = "blocks", value = "1"); add_col!(df, col = "rows", value = "1"); add_col!(df, col = "cols", value = "1"); add_col!(df, col = "experiments", value = "exp-42"); add_col!(df, col = "treatments", value = "trt-42");

julia> add_measurement_dates!(df);

julia> insert_layouts!(conn, df = df, is_trial = false); insert_names!(conn, df = df, table = "experiments", df_col = "experiments"); insert_names!(conn, df = df, table = "treatments", df_col = "treatments"); insert_names!(conn, df = df, table = "sites", df_col = "sites"); insert_names!(conn, df = df, table = "measurements", df_col = "measurements");

julia> environment_variables = extract_environment_variables(df);

julia> insert_names!(conn, df = DataFrame(environment_variables = environment_variables), table = "environment_variables", df_col = "environment_variables");

julia> insert_environment_data!(conn, df=df, environment_variables=environment_variables);

julia> execute(conn, "SELECT id,value FROM environment_data") |> DataFrame |> nrow > 0
true

julia> close(conn);

```
"""
function insert_environment_data!(
    conn::LibPQ.Connection;
    df::DataFrame,
    environment_variables::Vector{String},
    verbose::Bool = false,
)
    # conn = dbconnect(); fname = simulate_trial() |> simulate_environment; missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]; experiment="some-exp"; treatment="some_trt"; df = load_environments_df(fname, missing_strings=missing_strings); measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in [string(x) for x in unique(df.measurements)]]; verbose::Bool = true
    # df = load_environments_df(fname, missing_strings = missing_strings)
    # add_col!(df, col = "replications", value = "1")
    # add_col!(df, col = "blocks", value = "1")
    # add_col!(df, col = "rows", value = "1")
    # add_col!(df, col = "cols", value = "1")
    # add_col!(df, col = "experiments", value = experiment)
    # add_col!(df, col = "treatments", value = treatment)
    # add_measurement_dates!(df; measurement_dates = measurement_dates)
    # insert_layouts!(conn, df = df, is_trial = false)
    # insert_names!(conn, df = df, table = "experiments", df_col = "experiments", verbose = verbose)
    # insert_names!(conn, df = df, table = "treatments", df_col = "treatments", verbose = verbose)
    # insert_names!(conn, df = df, table = "sites", df_col = "sites", verbose = verbose)
    # insert_names!(conn, df = df, table = "measurements", df_col = "measurements", verbose = verbose)
    # environment_variables = extract_environment_variables(df, verbose = verbose)
    # insert_names!(conn, df = DataFrame(environment_variables = environment_variables), table = "environment_variables", df_col = "environment_variables", verbose = verbose)
    check(conn, "environment_data")
    tables = ["experiments", "sites", "treatments", "layouts", "measurements", "environment_variables"]
    names_in_db::Dict{String,DataFrame} = Dict()
    errors = String[]
    for table in tables
        # table = tables[end]
        names_df = if table != "environment_variables"
            String.(unique(df[!, table]))
        else
            environment_variables
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
    pb = ProgressMeter.Progress(nrow(df)*length(environment_variables), "Importing environment data...")
    execute(conn, "BEGIN")
    try
        for i = 1:nrow(df)
            # i = 1
            # println(i)
            experiment_id = filter(x->x.name==df.experiments[i], names_in_db["experiments"]).id[1]
            site_id = filter(x->x.name==df.sites[i], names_in_db["sites"]).id[1]
            treatment_id = filter(x->x.name==df.treatments[i], names_in_db["treatments"]).id[1]
            layout_id = filter(x->x.name==df.layouts[i], names_in_db["layouts"]).id[1]
            measurement_id = filter(x->x.name==df.measurements[i], names_in_db["measurements"]).id[1]
            for environment_variable in environment_variables
                # environment_variable = environment_variables[2]
                # println(environment_variable)
                environment_variable_id =
                    filter(x->x.name==environment_variable, names_in_db["environment_variables"]).id[1]
                y = !ismissing(df[i, environment_variable]) ? df[i, environment_variable] : NaN
                # y = NaN
                execute(
                    conn,
                    """
                    INSERT INTO environment_data
                    (
                        experiment_id,
                        site_id,
                        treatment_id,
                        layout_id,
                        measurement_id,
                        environment_variable_id,
                        value
                    )
                    VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7)
                    ON CONFLICT 
                    (
                        experiment_id,
                        site_id,
                        treatment_id,
                        layout_id,
                        measurement_id,
                        environment_variable_id
                    ) DO NOTHING
                    """,
                    [experiment_id, site_id, treatment_id, layout_id, measurement_id, environment_variable_id, y],
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
    # execute(conn, "SELECT id,value FROM environment_data") |> DataFrame
    nothing
end
