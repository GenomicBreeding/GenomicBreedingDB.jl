"""
    dbconnect()::LibPQ.Connection

Create a connection to the configured PostgreSQL database and validate the
database schema.

The function loads database connection settings from a `.env` file located in the
user's home directory and establishes a connection using `LibPQ`. After
connecting, a series of schema validation checks are performed to ensure that
required database tables contain the expected `name` field.

If the connection cannot be established or the database schema does not satisfy
the required conventions, an informative error is raised.

# Returns

- `LibPQ.Connection`: Active PostgreSQL database connection.

# Throws

- `ErrorException`: If the database connection cannot be established.
- `ErrorException`: If required environment variables are missing.
- `Exception`: If one or more required database tables do not contain a `name`
  field.
- Any exception raised while querying database metadata.

# Notes

- Connection details are loaded from `~/.env` using `DotEnv.load!`.
- The following environment variables are required:
  `DB_NAME`, `DB_USER`, `DB_PASSWORD`, and `DB_HOST`.
- Database connections are established using `LibPQ.Connection`.
- After connecting, the schema is validated using metadata from
  `information_schema`.
- The following tables are expected to contain a mandatory `name` field:
  `species`, `entries`, `experiments`, `sites`, `treatments`, `traits`,
  `measurements`, `reference_genomes`, `genotype_vcfs`, `genomes`,
  `phenomes`, and `fits`.
- The function serves as both a connection helper and a schema-validation
  safeguard for downstream database operations.
"""
function dbconnect()::LibPQ.Connection
    DotEnv.load!(joinpath(homedir(), ".env"))
    db_name = ENV["DB_NAME"]
    db_user = ENV["DB_USER"]
    db_password = ENV["DB_PASSWORD"]
    db_host = ENV["DB_HOST"]
    conn = try
        LibPQ.Connection("dbname=$db_name user=$db_user password=$db_password host=$db_host")
    catch e
        errors = [
            "Please make sure the \"$db_name\" database, and \"$db_user\" user exist, as well as the password and \"$db_host\" host port are correct!",
            sprint(showerror, e),
        ]
        error(join(string.("\n\t- ", errors)))
    end
    df_tables_without_names = DataFrame(
        execute(
            conn,
            """
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
            AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns c
                WHERE c.table_schema = t.table_schema
                AND c.table_name IN ('species', 'entries', 'experiments', 'sites', 'treatments', 'traits', 'measurements', 'reference_genomes', 'genotype_vcfs', 'genomes', 'phenomes', 'fits')
                AND c.column_name = 'name'
            )
            ORDER BY t.table_schema, t.table_name;
            """,
        ),
    )
    if nrow(df_tables_without_names) > 0
        throw(
            "Invalid tables: [$(join(df_tables_without_names.table_name, ", "))], i.e. missing the mandatory 'name' field in ['species', 'entries', 'experiments', 'sites', 'treatments', 'traits', 'measurements', 'reference_genomes', 'genotype_vcfs', 'genomes', 'phenomes', 'fits'].",
        )
    end
    return conn
end

"""
    dbinit(
        schema_path::String="db/schema.sql",
    )::Nothing

Initialise the database schema from a SQL definition file.

The function establishes a database connection, reads the specified schema file,
and executes each SQL statement sequentially. Special handling is provided for
PostgreSQL function definitions so that multi-statement function bodies are
processed correctly despite containing internal semicolons.

Errors encountered during schema creation are collected and reported after all
statements have been processed. Certain expected errors related to existing enum
types may be tolerated, whilst unexpected errors cause database initialisation to
fail.

# Arguments

- `schema_path::String="db/schema.sql"`: Path to the SQL schema file used to
  initialise the database.

# Returns

- `Nothing`: Database schema objects are created or updated in place.

# Throws

- `ErrorException`: If the schema file cannot be read.
- `ErrorException`: If database initialisation encounters unexpected errors.
- Any exception raised while connecting to the database.
- Any exception raised while executing schema statements that prevent successful
  initialisation.

# Notes

- A database connection is established using `dbconnect`.
- SQL statements are executed sequentially from the supplied schema file.
- PostgreSQL function definitions are reconstructed before execution to handle
  embedded semicolons correctly.
- Failed statements trigger a transaction rollback before processing continues.
- Errors are accumulated and evaluated after all statements have been processed.
- Errors associated with existing `entry_type` or `relationship_type`
  definitions may be treated as non-fatal.
- Database connections are closed before the function exits.
- The function is intended for schema creation and maintenance rather than
  routine database operations.
"""
function dbinit(schema_path::String = "db/schema.sql")::Nothing
    # schema_path = "db/schema.sql"
    conn = dbconnect()
    sql = read(schema_path, String)
    errors = []
    psql_function = String[]
    for stmt in split(sql, ';')
        # stmt = split(sql, ';')[1]
        # stmt = split(sql, ';')[4]
        stmt = strip(stmt)
        stmt = if .!isnothing(match(Regex("[\$]"), stmt)) && (length(psql_function) > 0)
            # Function end
            psql_function = push!(psql_function, stmt)
            stmt = join(psql_function, "; ")
            psql_function = String[]
            stmt
        elseif .!isnothing(match(Regex("[\$]"), stmt)) || (length(psql_function) > 0)
            # Function start or body
            psql_function = push!(psql_function, stmt)
            continue
        else
            # Not a function
            stmt
        end
        isempty(stmt) && continue
        # println(stmt)
        try
            execute(conn, stmt * ";")
        catch e
            println("An error occurred! Rolling back transaction.")
            execute(conn, "ROLLBACK;")
            # rethrow(e)
            push!(errors, e)
            continue
        end
    end
    errors = [x.msg for x in errors]
    if (length(errors) > 0) &&
       (sum(.!isnothing.(match.(Regex("entry_type|relationship_type"), errors))) < length(errors))
        println("At least one error occurred! Resetting the database!")
        close(conn)
        throw(join(errors, ""))
    end
    close(conn)
end
