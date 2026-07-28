"""
    check(
        conn::LibPQ.Connection,
    )::Nothing

Validate that a database connection is open and available for use.

The function checks whether the supplied PostgreSQL connection has been closed.
If the connection is no longer active, an error is raised instructing the user
to establish a new connection before continuing.

This validation function is intended to be used defensively before performing
database operations that require an active connection.

# Arguments

- `conn::LibPQ.Connection`: PostgreSQL database connection to validate.

# Returns

- `Nothing`: Returned when the connection is open and usable.

# Throws

- `ErrorException`: If the database connection has been closed.

# Notes

- The connection state is determined using the `closed` property of the
  `LibPQ.Connection` object.
- The function performs validation only and does not modify the connection.
- This check can be used before executing queries, updates, inserts, or other
  database operations that require an active session.
- A closed connection cannot be reopened and must be replaced with a new
  connection created via `dbconnect()`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try isnothing(check(conn)); catch; false; end
true

julia> close(conn);

julia> try isnothing(check(conn)); catch; false; end
false
````
"""
function check(conn::LibPQ.Connection)::Nothing
    if conn.closed.value
        error("The connection to the database is closed! Please open a new connection!")
    end
    nothing
end

"""
    check(
        conn::LibPQ.Connection,
        table::String,
    )::Nothing

Validate that a database connection is open and that a specified table exists in
the database.

The function first verifies that the supplied PostgreSQL connection is active
using `check(conn)`. It then validates the table name against the project's naming
conventions and confirms that the table exists within the `public` schema of the
connected database.

Table existence is determined using PostgreSQL's `to_regclass` function, which
provides a reliable mechanism for identifying registered database objects.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table to validate.

# Returns

- `Nothing`: Returned when the connection is open and the table exists.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If the table name contains illegal characters or strings.
- `ErrorException`: If the specified table does not exist in the database.
- Any database exception raised while checking table existence.

# Notes

- Connection validation is delegated to `check(conn)`.
- Table-name validation is performed using `check_illegal_strings`.
- Existence checks are performed against the `public` schema.
- PostgreSQL's `to_regclass` function is used to determine whether the table is
  present in the database.
- The function performs validation only and does not modify the database.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try isnothing(check(conn, "entries")); catch; false; end
true

julia> try isnothing(check(conn, "this_table_does_not_exist")); catch; false; end
false

julia> close(conn);
```
"""
function check(conn::LibPQ.Connection, table::String)::Nothing
    # conn = dbconnect(); table = "rgsg"
    check(conn)
    check_illegal_strings([table])
    bool =
        execute(conn, "SELECT to_regclass('public.$table') IS NOT NULL AS table_exists") |>
        DataFrame |>
        x -> x.table_exists[1]
    if !bool
        error("The \"$table\" table does not exist in the database!")
    end
    nothing
end

"""
    check(
        conn::LibPQ.Connection,
        table::String,
        field::String,
    )::Nothing

Validate that a database connection is open and that a specified field exists
within a database table.

The function first verifies that the supplied PostgreSQL connection is active
using `check(conn)`. It then validates both the table and field names against the
project's naming conventions before confirming that the specified field exists in
the target table and has not been dropped.

Field existence is determined using PostgreSQL system catalogue metadata stored
in `pg_attribute`.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table containing the field.
- `field::String`: Name of the field to validate.

# Returns

- `Nothing`: Returned when the connection is open and the specified field exists
  in the target table.

# Throws

- `ErrorException`: If the database connection has been closed.
- `ErrorException`: If the table name contains illegal characters or strings.
- `ErrorException`: If the field name contains illegal characters or strings.
- `ErrorException`: If the specified field does not exist in the target table.
- Any database exception raised whilst checking field existence.

# Notes

- Connection validation is delegated to `check(conn)`.
- Table and field names are validated using `check_illegal_strings`.
- Field existence is determined using PostgreSQL system catalogue information in
  `pg_attribute`.
- Dropped fields are excluded from the existence check using
  `NOT attisdropped`.
- The target table is resolved using PostgreSQL's `regclass` mechanism.
- The function performs validation only and does not modify the database.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try isnothing(check(conn, "entries", "name")); catch; false; end
true

julia> try isnothing(check(conn, "entries", "this_field_does_not_exist")); catch; false; end
false

julia> close(conn);
```
"""
function check(conn::LibPQ.Connection, table::String, field::String)::Nothing
    # conn = dbconnect(); table = "phenotype_data"; field = "site_id"; # field = "site"
    check(conn)
    check_illegal_strings([table])
    check_illegal_strings([field])
    bool = execute(
        conn,
        """
        SELECT EXISTS (
            SELECT 1 
            FROM pg_attribute 
            WHERE attrelid = 'public.$table'::regclass 
            AND attname = '$field'
            AND NOT attisdropped
        );
        """,
    ) |> DataFrame |> x -> x.exists[1]
    if !bool
        error("The \"$field\" field does not exist in the\"$table\" table!")
    end
    nothing
end

"""
    check(
        conn::LibPQ.Connection,
        table::String,
        field::String,
        T::Type,
    )::Nothing

Validate that a database field exists and is compatible with an expected Julia
type.

The function verifies that the specified field exists in the target table and
queries PostgreSQL to determine the underlying database type associated with the
field. The detected database type is then compared against an expected Julia
type category, allowing validation of string, numeric, and temporal fields
before they are used in filtering, querying, or update operations.

This function is primarily intended for defensive schema validation and helps
ensure that higher-level database operations are applied only to fields whose
types are compatible with the requested operation.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table containing the field.
- `field::String`: Name of the field to validate.
- `T::Type`: Expected Julia type category.

# Returns

- `Nothing`: Returned when the field exists and its database type is compatible
  with the expected Julia type.

# Throws

- `ErrorException`: If the specified table does not exist.
- `ErrorException`: If the specified field does not exist.
- `ErrorException`: If a string type is requested but the field is not a
  supported string-compatible database type.
- `ErrorException`: If a numeric type is requested but the field is not a
  supported numeric database type.
- `ErrorException`: If a date or datetime type is requested but the field is
  not a supported temporal database type.
- Any database exception raised whilst retrieving schema information.

# Notes

- Table and field validation are performed using
  `check(conn, table, field)`.
- PostgreSQL field types are determined using `pg_typeof(...)`.
- String validation (`T <: AbstractString`) supports:
  - `text`
  - `entry_type`
  - `relationship_type`
- Enum fields such as `entry_type` and `relationship_type` are treated as
  string-compatible types and may be used with string-based filtering
  operations.
- Numeric validation (`T <: Number`) supports:
  - `smallint`
  - `int`
  - `integer`
  - `numeric`
  - `decimal`
  - `real`
  - `double precision`
  - `smallserial`
  - `serial`
  - `bigserial`
- Temporal validation (`T <: Date` or `T <: DateTime`) supports:
  - `timestamp`
  - `timestamptz`
  - `date`
  - `time`
  - `timetz`
  - `interval`
- The function validates compatibility with broad type categories rather than
  exact Julia types.
- No database contents are modified.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> try isnothing(check(conn, "entries", "name", String)); catch; false; end
true

julia> try isnothing(check(conn, "entries", "name", Int)); catch; false; end
false

julia> try isnothing(check(conn, "layouts", "replication", Int)); catch; false; end
true

julia> try isnothing(check(conn, "layouts", "row", Int)); catch; false; end
true

julia> try isnothing(check(conn, "layouts", "col", Int)); catch; false; end
true

julia> try isnothing(check(conn, "phenotype_data", "value", Float64)); catch; false; end
true

julia> close(conn);
```
"""
function check(conn::LibPQ.Connection, table::String, field::String, T::Type)::Nothing
    # conn = dbconnect(); table = "entries"; field = "entry_type"; T = String
    check(conn, table, field)
    t = execute(
        conn,
        """
        SELECT pg_typeof($field) 
        FROM $table 
        LIMIT 1;
        """
    ) |> DataFrame |> x -> x.pg_typeof[1]
    if (
        (T <: AbstractString) && 
        (t != "text") &&
        (t != "entry_type") &&
        (t != "relationship_type")
    )
        error("The \"$field\" field in table \"$table\" is not string!")
    end
    if (
        (T <: Number) && 
        (t != "smallint") && 
        (t != "int") && 
        (t != "integer") && 
        (t != "numeric") && 
        (t != "decimal") && 
        (t != "real") && 
        (t != "double precision") && 
        (t != "smallserial") && 
        (t != "serial") && 
        (t != "bigserial")
    )
        error("The \"$field\" field in table \"$table\" is not numeric!")
    end
    if (
        (
            (T <: Date) ||
            (T <: DateTime)
        ) && 
        (t != "timestamp") && 
        (t != "timestamptz") && 
        (t != "date") && 
        (t != "time") && 
        (t != "timetz") && 
        (t != "interval")
    )
        error("The \"$field\" field in table \"$table\" is not date or datetime!")
    end
    nothing
end