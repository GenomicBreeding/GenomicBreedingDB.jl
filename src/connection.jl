"""
    dbconnect()::LibPQ.Connection

Establishes a connection to a PostgreSQL database using environment variables.

# Returns
- `LibPQ.Connection`: A connection object to the PostgreSQL database

# Environment Variables Required
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password 
- `DB_NAME`: Name of the database
- `DB_HOST`: Database host address
"""
function dbconnect()::LibPQ.Connection
    db_user = ENV["DB_USER"]
    db_password = ENV["DB_PASSWORD"]
    db_name = ENV["DB_NAME"]
    db_host = ENV["DB_HOST"]
    conn = LibPQ.Connection(
        "dbname=$db_name user=$db_user password=$db_password host=$db_host",
    )
    return conn
end

"""
    dbinit(schema_path::String = "db/schema.sql")::Nothing

Initialize the database by executing SQL statements from a schema file.

This function connects to the database, reads SQL statements from the specified schema file,
and executes them sequentially. Each statement in the file should be separated by semicolons.

# Arguments
- `schema_path::String`: Path to the SQL schema file. Defaults to "db/schema.sql"

# Returns
- `Nothing`: Function performs database operations but does not return a value
"""
function dbinit(schema_path::String = "db/schema.sql")::Nothing
    # schema_path = "db/schema.sql"
    conn = dbconnect()
    sql = read(schema_path, String)
    for stmt in split(sql, ';')
        stmt = strip(stmt)
        if !isempty(stmt)
            execute(conn, stmt)
        end
    end
    close(conn)
end
