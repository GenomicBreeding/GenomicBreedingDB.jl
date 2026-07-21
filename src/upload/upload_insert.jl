"""
    insert_names!(conn::LibPQ.Connection; df::DataFrame, table::String, df_col::String, verbose::Bool = false)::Nothing

Insert new names from a DataFrame column into a specified database table.

# Arguments
- `conn::LibPQ.Connection`: Database connection object
- `df::DataFrame`: DataFrame containing the names to insert
- `table::String`: Target table name in the database
- `df_col::String`: Column name in the DataFrame to extract names from
- `verbose::Bool = false`: If true, display progress information during insertion

# Throws
- `String`: If the specified column `df_col` does not exist in the DataFrame
- `String`: If the specified `table` does not exist in the database or lacks a 'name' field
- `String`: If the column contains illegal characters or non-ASCII content (see `check_illegal_strings()` for details on allowed characters)

# Details
This function performs the following operations:
1. Validates that the specified column exists in the DataFrame
2. Validates that all names in the column contain only allowed characters using `check_illegal_strings()`
   - Illegal characters: `;`, `|`, `,`, `.`, `/`, `\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`, `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`, `?`
   - Non-ASCII characters are rejected
3. Extracts, sorts, and deduplicates names from the specified column
4. Retrieves existing names from the database table
5. Inserts only new names that don't already exist in the table
6. Uses database transactions with rollback on error

The function maintains data integrity through transaction handling (BEGIN/COMMIT/ROLLBACK).
Progress tracking is displayed if `verbose=true`.

# Returns
`Nothing`

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

Insert unique layout positions from a DataFrame into the `layouts` table.

The function standardises layout information using @ref,
extracts unique layout combinations, and inserts them into the database within
a single transaction.

Each unique layout is represented by:

- `replication`
- `block`
- `row`
- `col`

and is assigned a layout name of the form:

`"<replication>-<block>-<row>-<col>"`.

Rows that already exist in the `layouts` table are ignored using
`ON CONFLICT (name) DO NOTHING`.

# Arguments

- `conn::LibPQ.Connection`: An open PostgreSQL connection.
- `df::DataFrame`: A DataFrame containing layout information.

# Keyword Arguments

- `is_trial::Bool=true`: If `true`, validates the input data through
  `parse_layouts!(df; is_trial=true)` before insertion. Set to `false`
  to skip validation.
- `verbose::Bool=false`: If `true`, displays insertion progress and
  completion information.

# Details

The function:

1. Calls `parse_layouts!()` to ensure the columns
   `:replications`, `:blocks`, `:rows`, and `:cols`
   contain integer values and that the `:layouts` column exists.
2. Identifies unique layout names from `df.layouts`.
3. Splits each layout name into its replication, block, row, and column
   components.
4. Inserts each unique layout into the `layouts` table.

# Transaction Behaviour

All inserts are executed inside a single database transaction.

- On success, the transaction is committed.
- On failure, the transaction is rolled back and the original exception is
  rethrown.

# Returns

`Nothing`.

# Throws

Any exception generated by the database connection, query execution,
transaction handling, or layout parsing is propagated to the caller after a
transaction rollback.

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
    insert_entry_relationships!(conn::LibPQ.Connection; df::DataFrame, verbose::Bool=false)::Nothing

Insert entry relationship records into the database from a DataFrame.

# Arguments
- `conn::LibPQ.Connection`: Database connection object
- `df::DataFrame`: Input DataFrame containing relationship data
- `verbose::Bool=false`: If true, display a progress meter during insertion

# Required DataFrame Columns
- `entries`: Names of child entries
- `populations`: Names of parent entries (populations)
- `relationship_types`: Types of relationships between entries and populations, i.e.:
    + `member_of`
    + `clone_of`
    + `parent_is`
    + `maternal_parent_is`
    + `paternal_parent_is`

# Behaviour
- Validates that all required columns are present in the input DataFrame
- Makes sure there are no duplicate relationships to te inserted based on the combination of entries, populations, and relationship_types
- Inserts each unique relationship into the `entry_relationships` table
- Uses `ON CONFLICT DO NOTHING` to skip duplicate entries (based on child_id, parent_id, rel_type constraints)
- Wraps all operations in a transaction; rolls back on error

# Throws
- `String`: If required columns are missing from the DataFrame

# Returns
- `Nothing`

# Example

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
    insert_phenotype_data!(conn::LibPQ.Connection; df::DataFrame, traits::Vector{String}, verbose::Bool=false)

Insert phenotype data from a DataFrame into the database.

# Arguments
- `conn::LibPQ.Connection`: Database connection object
- `df::DataFrame`: DataFrame containing phenotype data with columns for the names of:
    + entries,
    + experiments,
    + sites,
    + treatments,
    + layouts,
    + measurements, and
    + traits
- `traits::Vector{String}`: Vector of trait column names to insert
- `verbose::Bool=false`: If true, displays a progress bar during insertion

# Description
This function inserts phenotype measurements into the database by:
1. Extracting IDs for all referenced entities (entries, experiments, sites, treatments, layouts, measurements, traits)
2. Iterating through each row and trait combination
3. Inserting or skipping (on conflict) phenotype records into the `phenotype_data` table
4. Handling missing values as NaN

The function uses database transactions for data consistency, where all inserts are committed together or rolled back on error.

# Example

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

Insert environmental measurements into the `environment_data` table.

This function inserts environmental observations from a DataFrame into the
database. Each environmental value is linked to an experiment, site,
treatment, layout, measurement, and environmental variable using the
corresponding identifiers stored in the database.

# Arguments

- `conn::LibPQ.Connection`: An open PostgreSQL connection.
- `df::DataFrame`: A DataFrame containing environmental measurements and
  associated metadata.
- `environment_variables::Vector{String}`: Names of environmental-variable
  columns in `df` to import.

# Keyword Arguments

- `verbose::Bool=false`: If `true`, displays a progress bar while importing
  data.

# Details

The function expects the following lookup tables to have already been
initialised and populated:

- `experiments`
- `sites`
- `treatments`
- `layouts`
- `measurements`
- `environment_variables`

For each row of `df`, the function:

1. Looks up the corresponding identifiers for the experiment, site,
   treatment, layout, and measurement.
2. Iterates over each environmental variable listed in
   `environment_variables`.
3. Looks up the identifier of the environmental variable.
4. Inserts the environmental value into the `environment_data` table.

Missing values are converted to `NaN` before insertion.

Duplicate records are ignored using the unique constraint on

`(experiment_id, site_id, treatment_id, layout_id, measurement_id, environment_variable_id)`.

# Transaction Behaviour

All inserts are performed within a single database transaction.

- On success, the transaction is committed.
- On failure, the transaction is rolled back and the original exception is
  rethrown.

# Returns

`Nothing`.

# Throws

- `ErrorException`: If one or more required lookup tables have not been
  initialised.
- Any exception generated during identifier lookup, query execution, or
  transaction handling.

# Example

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
