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
    check(conn, table) # checks for illegal strings in $table
    check(conn, table, "name") # checks for illegal strings in $table
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
                error("Invalid relationship type: \"$rel_type\".")
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
