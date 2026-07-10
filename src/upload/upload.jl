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
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

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
    # df = CSV.read(simulate(), DataFrame)
    # table = "entries"
    # df_col = "entries"
    # verbose::Bool = true
    if df_col∉names(df)
        error(
            "The \"$df_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    try
        check_illegal_strings(String.(unique(df[!, df_col])))
    catch e
        new_error =
            join(["Illegal string in the \"$df_col\" column!\n", sprint(showerror, e)])
        error(new_error)
    end
    uploaded_names =
        select(df, [Symbol(df_col)])[:, 1] |> x -> String.(string.(x)) |> sort |> unique
    existing_names = let
        df_tmp = try
            DataFrame(execute(conn, "SELECT name FROM $table;"))
        catch
            error(
                join(
                    "Missing \"$table\" table in the database!\n",
                    "(Note that the existence of the 'name' field is checked every time a connection to the database is made via `dbconnect()`,\n",
                    "i.e. for the following tables: 'species', 'entries', 'experiments', 'sites', 'treatments', 'traits', 'measurements', 'reference_genomes', 'genotype_vcfs', 'genomes', 'phenomes', 'fits')",
                ),
            )
        end
        String.(string.(df_tmp[:, 1]))
    end
    counter = 0
    pb = ProgressMeter.Progress(
        length(uploaded_names),
        "Inserting names listed in \"$df_col\" into \"$table\" table...",
    )
    execute(conn, "BEGIN")
    try
        for x in uploaded_names
            # x = uploaded_names[1]
            if x ∉ existing_names
                execute(
                    conn,
                    """
                    INSERT INTO $table (name)
                    VALUES (\$1);
                    """,
                    [x],
                )
                counter += 1
                verbose ? ProgressMeter.next!(pb) : nothing
            end
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
    update_table_field_by_name!(conn::LibPQ.Connection; df::DataFrame, table::String, df_name_col::String, df_source_col::String, table_destination_field::String, verbose::Bool = false)::Nothing

Update a specific field in a database table by matching records based on a name column in a DataFrame.

# Arguments
- `conn::LibPQ.Connection`: Active database connection.
- `df::DataFrame`: Source DataFrame containing the data to update.
- `table::String`: Name of the target table in the database.
- `df_name_col::String`: Name of the column in `df` used to match records in the database table via the `name` field.
- `df_source_col::String`: Name of the column in `df` containing the values to update.
- `table_destination_field::String`: Name of the field in the database table to update. If the field name ends with "_id", the function will resolve foreign key references from the corresponding related table.
- `verbose::Bool`: If `true`, displays a progress bar during the update operation. Defaults to `false`.

# Returns
- `Nothing`

# Behaviour
- Validates that both source columns exist in the provided DataFrame.
- Validates that the target table and field exist in the database.
- Validates string columns using `check_illegal_strings()` to ensure they contain only allowed characters for database identifiers and names. The following characters are not allowed: `;`, `|`, `,`, `.`, `/`, `\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`, `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`, `?`. Non-ASCII characters are also rejected.
- Automatically handles foreign key relationships when the destination field ends with "_id" by looking up IDs from the related table.
- Updates the `updated_at` timestamp for each modified record.
- Uses database transactions (BEGIN/COMMIT/ROLLBACK) to ensure atomicity.
- Throws an error if the number of affected rows is unexpected (i.e. ero or more than 1) or if the table is empty.
- Displays a progress meter if verbose mode is enabled.

# Throws
- `String`: If required columns don't exist in the DataFrame.
- `String`: If string columns contain illegal characters or non-ASCII content (see `check_illegal_strings()` for details).
- `String`: If the table or field doesn't exist in the database.
- `String`: If the table is empty before updating.
- `String`: If an unexpected number of rows are affected during update.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> df.entries = string.("test_update_table-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> df.species .= string.("test_update_table-", Dates.time() |> x -> replace(string(x), "." => "_"), "-Elysia chlorotica");

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> insert_names!(conn, df=df, table="species", df_col="species");

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> update_table_field_by_name!(conn, df=df, table="entries", df_name_col="entries", df_source_col="species", table_destination_field="species_id");

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entries") |> DataFrame;

julia> close(conn);

julia> sum(.!ismissing.(unique(df_before.species_id))) < sum(.!ismissing.(unique(df_after.species_id)))
true
```
"""
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
    if df_name_col∉names(df)
        error(
            "The \"$df_name_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    if df_source_col∉names(df)
        error(
            "The \"$df_source_col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    try
        check_illegal_strings(String.(unique(df[!, df_name_col])))
    catch e
        new_error =
            join(["Illegal string in the \"$df_name_col\" column!\n", sprint(showerror, e)])
        error(new_error)
    end
    if eltype(df[!, df_source_col]) <: AbstractString
        try
            check_illegal_strings(String.(unique(df[!, df_source_col])))
        catch e
            new_error = join([
                "Illegal string in the \"$df_source_col\" column!\n",
                sprint(showerror, e),
            ])
            error(new_error)
        end
    end
    table_exists =
        nrow(DataFrame(execute(
            conn,
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = \$1
            """,
            [table],
        ))) > 0
    if !table_exists
        error("The \"$table\" table does not exist in the database!")
    end
    field_exists =
        nrow(
            DataFrame(
                execute(
                    conn,
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = \$1
                    AND column_name = \$2
                    """,
                    [table, table_destination_field],
                ),
            ),
        ) > 0
    if !field_exists
        error(
            "The \"$table_destination_field\" field does not exist in the \"$table\" table!",
        )
    end
    # We extract the ids if we need to update the ids from some related table, i.e. if we have the pattern "*_id" for the `table_destination_field`
    df_tmp = if split(table_destination_field, "_")[end] == "id"
        df_tmp = unique(select(df, [df_name_col, df_source_col]))
        root_table = join(split(table_destination_field, "_")[1:(end-1)], "_")
        root_table = if root_table == "entry"
            "entries"
        elseif root_table[end] != 's'
            root_table * "s"
        else
            root_table
        end
        ids = String[]
        for x in String.(string.(df_tmp[!, df_source_col]))
            # x = df_tmp[!, df_source_col][1]
            push!(
                ids,
                execute(conn, "SELECT id FROM $root_table WHERE name = \$1", [x]) |>
                DataFrame |>
                x -> first(x.id),
            )
        end
        df_tmp[!, df_source_col] = ids
        df_tmp
    else
        unique(select(df, [df_name_col, df_source_col]))
    end
    counter = 0
    pb = ProgressMeter.Progress(
        nrow(df_tmp),
        desc = "Updating $(nrow(df_tmp)) values of the \"$table_destination_field\" field in the \"$table\" table at ...",
    )
    execute(conn, "BEGIN")
    try
        bool =
            execute(conn, "SELECT EXISTS ( SELECT 1 FROM $table)") |>
            DataFrame |>
            x -> x.exists[1]
        if !bool
            error(
                "The \"$table\" table is empty! Please populate the \"name\" field first before updating the other fields using the \"name\" field.",
            )
        end
        for i = 1:nrow(df_tmp)
            # i = 1
            res = execute(
                conn,
                """
                UPDATE $table
                SET
                    $table_destination_field = \$1,
                    updated_at = now()
                WHERE name = \$2
                ;
                """,
                [df_tmp[i, df_source_col], df_tmp[i, df_name_col]],
            )
            if LibPQ.num_affected_rows(res) != 1
                error(
                    "Unexepcted number of rows affected, i.e. affecting $(LibPQ.num_affected_rows(res)) rows in \"$table\"!",
                )
            end
            counter += 1
            verbose ? ProgressMeter.next!(pb) : nothing
        end
        if verbose
            ProgressMeter.finish!(pb)
            println("Updated $counter rows in the \"$table\" table.")
        end
        execute(conn, "COMMIT")
    catch e
        execute(conn, "ROLLBACK")
        rethrow(e)
    end
    nothing
end

"""
    insert_layouts!(conn::LibPQ.Connection; df::DataFrame, verbose::Bool=false) -> Nothing

Insert unique layout positions from `df` into the `layouts` table.

The function extracts unique combinations of:

- `replications`
- `blocks`
- `rows`
- `cols`

from the supplied `DataFrame` and inserts them into the database within a
single transaction.

Rows that already exist in the `layouts` table (based on the unique
constraint `(replication, block, row, col)`) are silently ignored via
`ON CONFLICT DO NOTHING`.

# Arguments

- `conn::LibPQ.Connection`: An open PostgreSQL connection.
- `df::DataFrame`: A data frame containing the columns
  `replications`, `blocks`, `rows`, and `cols`.
- `verbose::Bool=false`: If `true`, display progress information and a
  summary after completion.

# Transaction Behaviour

All inserts are executed inside a single database transaction:

- On success, the transaction is committed.
- On error, the transaction is rolled back and the original exception is rethrown.

# Returns

- `Nothing`

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> conn = dbconnect();

julia> insert_layouts!(conn, df=df);

julia> n = execute(conn, "SELECT * FROM layouts") |> DataFrame |> nrow;

julia> m = string.(df.replications, "-", df.blocks, "-", df.rows, "-", df.cols) |> unique |> length;

julia> n == m
true
```
"""
function insert_layouts!(
    conn::LibPQ.Connection;
    df::DataFrame,
    verbose::Bool = false,
)::Nothing
    # conn::LibPQ.Connection = dbconnect()
    # df = load_trial_df(simulate())
    # verbose::Bool = true
    parse_layouts!(df)
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
            println(
                "Inserted $counter relationships between entries in the \"$table\" table.",
            )
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
    + `not_set_yet`

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
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> df.entries = string.("test_entry_rels-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.entries);

julia> df.populations = string.("test_entry_rels-", Dates.time() |> x -> replace(string(x), "." => "_"), "-", df.populations);

julia> df[!, "relationship_types"] .= "member_of";

julia> conn = dbconnect();

julia> insert_names!(conn, df=df, table="entries", df_col="entries")

julia> insert_names!(conn, df=df, table="entries", df_col="populations")

julia> df_before = LibPQ.execute(conn, "SELECT * FROM entry_relationships") |> DataFrame;

julia> insert_entry_relationships!(conn, df=df);

julia> df_after = LibPQ.execute(conn, "SELECT * FROM entry_relationships") |> DataFrame;

julia> close(conn);

julia> nrow(df_before) < nrow(df_after)
true
```
"""
function insert_entry_relationships!(
    conn::LibPQ.Connection;
    df::DataFrame,
    verbose::Bool = false,
)::Nothing
    expected_columns = ["entries", "populations", "relationship_types"]
    if sum([x∉names(df) for x in expected_columns]) > 0
        missing_columns = setdiff(expected_columns, names(df))
        if "relationship_types" ∈ missing_columns
            error(
                "We have missing columns: [\"$(join(missing_columns, "\", \""))\"]. (Hint: relationship_types: {\"member_of\", \"clone_of\", \"parent_is\", \"maternal_parent_is\", \"paternal_parent_is\", \"not_set_yet\"})",
            )
        else
            error("We have missing columns: [\"$(join(missing_columns, "\", \""))\"]")
        end
    end
    entry_population_relationship =
        string.(df.entries, "|||", df.populations, "|||", df.relationship_types) |>
        unique |>
        x -> split.(x, "|||")
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
            if rel_type∉[
                "member_of",
                "clone_of",
                "parent_is",
                "maternal_parent_is",
                "paternal_parent_is",
                "not_set_yet",
            ]
                error("Invalide relationship type: \"$rel_type\".")
            end
            child_id =
                execute(conn, "SELECT id FROM entries WHERE name = \$1", [child]) |>
                DataFrame |>
                x -> first(x.id)
            parent_id =
                execute(conn, "SELECT id FROM entries WHERE name = \$1", [parent]) |>
                DataFrame |>
                x -> first(x.id)
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
            println(
                "Inserted $counter relationships between entries in the \"entry_relationships\" table.",
            )
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
julia> conn = dbconnect();

julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

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
function insert_phenotype_data!(
    conn::LibPQ.Connection;
    df::DataFrame,
    traits::Vector{String},
    verbose::Bool = false,
)
    tables = [
        "entries",
        "experiments",
        "sites",
        "treatments",
        "layouts",
        "measurements",
        "traits",
    ]
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
            experiment_id =
                filter(x->x.name==df.experiments[i], names_in_db["experiments"]).id[1]
            site_id = filter(x->x.name==df.sites[i], names_in_db["sites"]).id[1]
            treatment_id =
                filter(x->x.name==df.treatments[i], names_in_db["treatments"]).id[1]
            layout_id = filter(x->x.name==df.layouts[i], names_in_db["layouts"]).id[1]
            measurement_id =
                filter(x->x.name==df.measurements[i], names_in_db["measurements"]).id[1]
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
                    [
                        entry_id,
                        experiment_id,
                        site_id,
                        treatment_id,
                        layout_id,
                        measurement_id,
                        trait_id,
                        y,
                    ],
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
    load_trial_data!(
        conn::LibPQ.Connection;
        fname::String,
        missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
        species::Union{Nothing, String} = nothing,
        experiment::Union{Nothing, String} = nothing,
        treatment::Union{Nothing, String} = nothing,
        measurement_dates::Union{Nothing, Dict{String, String}} = nothing,
        entry_type::Union{Nothing, String} = nothing,
        population_type::Union{Nothing, String} = nothing,
        relationship_type::Union{Nothing, String} = nothing,
        verbose::Bool = true,
    )::Nothing

Load and insert trial phenotype data into the GenomicBreedingDB database.

This is the primary function for uploading phenotypic trial data. 
It handles the complete pipeline of data validation, transformation, and insertion into 
the database, including species, experiments, treatments, sites, measurements, layouts, 
entries, traits, and phenotype values.

# Arguments
- `conn::LibPQ.Connection`: Active database connection for data insertion
- `fname::String`: Path to the input data file (supports both Trial struct format and CSV)
- `missing_strings::Vector{String}`: Missing value strings (default: `["missing", "NA", "na", "N/A", "n/a", ""]`)
- Include the following arguments if they are not present in the input dta file as separate columns:
    + `species::Union{Nothing, String}`: Species name to associate with the trial data
    + `experiment::Union{Nothing, String}`: Experiment identifier
    + `treatment::Union{Nothing, String}`: Treatment name or identifier
    + `measurement_dates::Union{Nothing, Dict{String, String}}`: Dictionary mapping measurement names to dates
    + `entry_type::Union{Nothing, String}`: Type of entries (e.g., "family", "individual")
    + `population_type::Union{Nothing, String}`: Type of population (e.g., "population", "panel")
    + `relationship_type::Union{Nothing, String}`: Type of relationships between entries
- `verbose::Bool`: Enable detailed logging of processing steps (default: `true`)

# Returns

- `Nothing`

# Details

The function performs the following operations in sequence:

1. **Data Loading**: Reads trial data from file (supports GenomicBreedingIO Trial struct format or CSV)
2. **Validation**: Ensures all required columns are present
3. **Layout Parsing**: Extracts layout information (replication, block, row, column)
4. **Metadata Assignment**: Adds species, experiment, treatment, entry type, and population type information
5. **Database Insertion**: Inserts or updates reference tables (species, experiments, treatments, sites, measurements, layouts, entries, traits)
6. **Field Updates**: Associates measurements with dates and layouts with spatial coordinates
7. **Entry Relationships**: Inserts pedigree/relationship data between entries
8. **Trait Extraction**: Identifies numeric phenotypic traits
9. **Phenotype Data**: Inserts individual phenotypic measurements linked to entries, experiments, sites, treatments, layouts, and measurement dates

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate(fname_output="test.tsv");

julia> df = load_trial_df(fname);

julia> measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); [measurement_dates[x] = x for x in [string(x) for x in unique(df.measurements)]];

julia> conn = dbconnect();

julia> try load_trial_data!(conn, fname=fname); catch; false; end
false

julia> load_trial_data!(conn, fname=fname, species="Acacia neglecta", experiment="some-exp", treatment="some_trt", measurement_dates=measurement_dates, entry_type="family", population_type="population", relationship_type="member_of");

julia> execute(conn, "SELECT * FROM species") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM entries") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM entry_relationships") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM experiments") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM sites") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM treatments") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM layouts") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM measurements") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM traits") |> DataFrame |> nrow > 0
true

julia> execute(conn, "SELECT * FROM phenotype_data") |> DataFrame |> nrow > 0
true

julia> close(conn); rm(fname);
```
"""
function load_trial_data!(
    conn::LibPQ.Connection;
    fname::String,
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
    species::Union{Nothing,String} = nothing,
    experiment::Union{Nothing,String} = nothing,
    treatment::Union{Nothing,String} = nothing,
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing,
    entry_type::Union{Nothing,String} = nothing,
    population_type::Union{Nothing,String} = nothing,
    relationship_type::Union{Nothing,String} = nothing,
    verbose::Bool = false,
)::Nothing
    # conn = dbconnect()
    # fname = simulate()
    # missing_strings::Union{String, Char, Vector{String}, Vector{Char}} = ["missing", "NA", "na", "N/A", "n/a", ""]
    # species::String = "Lolium multiflorum"
    # experiment::String = "STR_trial-2026"
    # treatment::String = "control"; verbose::Bool = true
    # measurement_dates::Union{Nothing, Dict{String, String}} = Dict(); df = CSV.read(fname, DataFrame); [measurement_dates[x] = x for x in ["$x" for x in unique(df.measurements)]]
    # entry_type::Union{Nothing, String} = "family"
    # population_type::Union{Nothing, String} = "population"
    # relationship_type::Union{Nothing, String} = "parent_is"
    # verbose::Bool = true
    if entry_type∉["cultivar", "population", "individual", "family", "not_set_yet"]
        error(
            "Invalid entry_type: \"$entry_type\". Choose from: [\"cultivar\", \"population\", \"individual\", \"family\", \"not_set_yet\"].",
        )
    end
    if population_type∉["cultivar", "population", "individual", "family", "not_set_yet"]
        error(
            "Invalid population_type: \"$population_type\". Choose from: [\"cultivar\", \"population\", \"individual\", \"family\", \"not_set_yet\"].",
        )
    end
    if relationship_type∉[
        "member_of",
        "clone_of",
        "parent_is",
        "maternal_parent_is",
        "paternal_parent_is",
        "not_set_yet",
    ]
        error(
            "Invalid relationship_type: \"$relationship_type\". Choose from: [\"member_of\", \"clone_of\", \"parent_is\", \"maternal_parent_is\", \"paternal_parent_is\", \"not_set_yet\"].",
        )
    end
    # Load the trial data which assumed by default to be in Trial struct delimited file format (see: https://genomicbreeding.github.io/GenomicBreedingIO.jl/stable/#GenomicBreedingIO.readdelimited-Tuple{Type{GenomicBreedingCore.Trials}})
    df = load_trial_df(fname, missing_strings = missing_strings)
    # Make sure we have all the required columns
    validate_trials(df)
    parse_layouts!(df)
    add_col!(df, col = "species", value = species)
    add_col!(df, col = "experiments", value = experiment)
    add_col!(df, col = "treatments", value = treatment)
    add_col!(df, col = "entry_types", value = entry_type)
    add_col!(df, col = "population_types", value = population_type)
    add_col!(df, col = "relationship_types", value = relationship_type)
    add_measurement_dates!(df; measurement_dates = measurement_dates)
    df[!, "notes_years_seasons"] = string.("seasons_", df.years, "-", df.seasons)
    # Insert layout information
    insert_layouts!(conn, df = df)
    # Insert the names if they do not yet exist
    insert_names!(conn, df = df, table = "species", df_col = "species", verbose = verbose)
    insert_names!(
        conn,
        df = df,
        table = "experiments",
        df_col = "experiments",
        verbose = verbose,
    )
    insert_names!(
        conn,
        df = df,
        table = "treatments",
        df_col = "treatments",
        verbose = verbose,
    )
    insert_names!(conn, df = df, table = "sites", df_col = "sites", verbose = verbose)
    insert_names!(
        conn,
        df = df,
        table = "measurements",
        df_col = "measurements",
        verbose = verbose,
    )
    # insert_names!(conn, df=df, table="layouts", df_col="layouts", verbose=verbose)
    insert_names!(conn, df = df, table = "entries", df_col = "entries", verbose = verbose)
    insert_names!(
        conn,
        df = df,
        table = "entries",
        df_col = "populations",
        verbose = verbose,
    )
    # insert_names!(conn, df=df, table="entries", df_col="layouts", verbose=verbose)
    # delete_names!(conn, df=df, table="entries", df_col="layouts", verbose=verbose)
    # execute(conn, "SELECT * FROM entries") |> DataFrame
    # Update the measurement dates
    update_table_field_by_name!(
        conn,
        df = df,
        table = "measurements",
        df_name_col = "measurements",
        df_source_col = "dates",
        table_destination_field = "measure_date",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "measurements",
        df_name_col = "measurements",
        df_source_col = "notes_years_seasons",
        table_destination_field = "notes",
        verbose = verbose,
    )
    # df_tmp = execute(conn, "SELECT * FROM measurements") |> DataFrame |> x -> select(x, [:name])
    # delete_names!(conn, df=df_tmp, table="measurements", df_col="name", verbose=verbose)
    # execute(conn, "SELECT * FROM measurements") |> DataFrame
    # Update the entries with their corresponding types and species
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "entries",
        df_source_col = "entry_types",
        table_destination_field = "entry_type",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "populations",
        df_source_col = "population_types",
        table_destination_field = "entry_type",
        verbose = verbose,
    )
    update_table_field_by_name!(
        conn,
        df = df,
        table = "entries",
        df_name_col = "entries",
        df_source_col = "species",
        table_destination_field = "species_id",
        verbose = verbose,
    )
    # execute(conn, "SELECT * FROM entries") |> DataFrame
    # Insert the relationships between entries and populations found in the data
    insert_entry_relationships!(conn, df = df, verbose = verbose)
    # execute(conn, "SELECT * FROM entry_relationships") |> DataFrame
    # ids_parents = execute(conn, "SELECT * FROM entry_relationships") |> DataFrame |> x -> unique(x.parent_id)
    # execute(conn, "SELECT * FROM entries WHERE id IN (\$1)", [join(ids_parents, ",")]) |> DataFrame
    # Extract the traits, i.e. numeric fields which are not layout or dates fields
    traits = extract_traits(df, verbose = verbose)
    insert_names!(
        conn,
        df = DataFrame(traits = traits),
        table = "traits",
        df_col = "traits",
        verbose = verbose,
    )
    # df_tmp = execute(conn, "SELECT name FROM traits") |> DataFrame |> x -> filter(xi -> isnothing(match(Regex("trait"), xi.name)), x)
    # delete_names!(conn, df=df_tmp, table="traits", df_col="name")
    # execute(conn, "SELECT * FROM traits") |> DataFrame
    # Finally, insert/update the phenotype data using the combinations of the ids each entry-experiment-site-treatment-layout-measurement combinations
    insert_phenotype_data!(conn, df = df, traits = traits, verbose = verbose)
    # execute(conn, "SELECT * FROM phenotype_data") |> DataFrame
    # execute(conn, "SELECT id, value FROM phenotype_data") |> DataFrame
    nothing
end
