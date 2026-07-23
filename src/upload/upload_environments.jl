"""
    insert_environment_data!(
        conn::LibPQ.Connection;
        df::DataFrame,
        environment_variables::Vector{String},
        verbose::Bool=false,
    )::Nothing

Insert environmental observations from a DataFrame into the `environment_data`
database table. This function is called by `upload_environment_data!(...)` which 
handles the input file loading and DataFrame preparation.

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
