"""
    dbconnect()::LibPQ.Connection

Establishes a connection to a PostgreSQL database using environment variables.

Validates that all tables in the database contain a mandatory 'name' column.
Throws an error if any table lacks this required field.

# Returns
- `LibPQ.Connection`: A connection object to the PostgreSQL database

# Environment Variables Required
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password 
- `DB_NAME`: Name of the database
- `DB_HOST`: Database host address

# Exceptions
- `String`: Throws if any non-system table is missing the mandatory 'name' column
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
    dbinit(schema_path::String = "db/schema.sql")::Nothing

Initialize the database by executing SQL statements from a schema file.

This function connects to the database, reads SQL statements from the specified schema file,
and executes them sequentially (except for functions, i.e. between dollar signs which gets
executed as a single string unit). Each statement in the file should be separated by semicolons.
If an error occurs during execution, the transaction is rolled back automatically.

# Arguments
- `schema_path::String`: Path to the SQL schema file. Defaults to "db/schema.sql"

# Returns
- `Nothing`: Function performs database operations but does not return a value

# Exceptions
- Errors during SQL execution will trigger a rollback and be re-thrown
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
    if (length(errors) > 0) && (
        sum(.!isnothing.(match.(Regex("entry_type|relationship_type"), errors))) < length(errors)
    )
        println("At least one error occurred! Resetting the database!")
        close(conn)
        throw(join(errors, ""))
    end
    close(conn)
end
