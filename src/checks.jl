
"""
    check_illegal_strings(
        x::Vector{String};
        additional_illegal_strings::Union{Nothing,Vector{String}} = nothing,
    )::Nothing

Validate that strings contain only characters permitted by the GenomicBreedingDB
naming conventions.

This function performs opinionated validation of identifiers and text values that
will be stored in database fields. It is primarily intended for validating names,
codes, identifiers, and other structured text values used throughout the database.
Free-text fields such as notes should typically not be validated with this function.

# Arguments
- `x::Vector{String}`: Strings to validate.
- `additional_illegal_strings::Union{Nothing,Vector{String}}=nothing`:
  Optional collection of additional patterns that should be treated as illegal.
  Each supplied string is interpreted as a regular expression and matched against
  every element of `x`.

# Validation Rules
Each string must satisfy all of the following:

- Contain only ASCII characters.
- Not contain any of the following characters:

  `;`, `|`, `,`, `.`, `/`, `\\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`,
  `\$`, `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`,
  `:`, `<`, `>`, `?`

- Not match any pattern supplied through
  `additional_illegal_strings`.

# Returns
- `nothing` if all validations succeed.

# Throws
- An exception if `x` is empty.
- An exception if any string contains non-ASCII characters.
- An exception if any string contains prohibited characters.
- An exception if any string matches a pattern in
  `additional_illegal_strings`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> x_legals = String["geno_1", "2026-07-08", "ABC-def_123-2026", "camelCase"];

julia> x_illegals = String["geno.1", "2026/07/08", "ABC|def_123-2026;", "#camelCase%", "∈LEMENT"];

julia> isnothing(check_illegal_strings(x_legals))
true

julia> try check_illegal_strings(x_illegals); catch; true; end
true
```
"""
function check_illegal_strings(
    x::Vector{String};
    additional_illegal_strings::Union{Nothing,Vector{String}} = nothing,
)::Nothing
    # This is a very opinionated check for strings/characters
    # Used to make sure we have consistent expectations on the type of names, and identifiers we have for any text (except notes) uploaded into the database tables
    # x = String["xdgdfg", "sdgdfgdf", "sdsdg.sdgdf"]
    # additional_illegal_strings = ["eno"]
    illegal_characters = Char[
        ';',
        '|',
        ',',
        '.',
        '/',
        '\\',
        '\"',
        '\'',
        '`',
        '~',
        '!',
        '@',
        '#',
        '\$',
        '%',
        '^',
        '&',
        '*',
        '(',
        ')',
        '+',
        '=',
        '{',
        '}',
        '[',
        ']',
        ':',
        '<',
        '>',
        '?',
    ] # plus any non-ascii characters
    if isempty(x)
        error("Vector of strings is empty")
    end
    errors = String[]
    for xi in x
        # xi = x[1]
        if !isascii(xi)
            xi_chars = collect(xi)
            idx_non_ascii = findall([!isascii(c) for c in xi_chars])
            push!(errors, "Non-ASCII character/s [$(join(xi_chars[idx_non_ascii], ", "))] in $xi.")
        end
        illegal_matches = collect(xi) ∩ illegal_characters
        if length(illegal_matches) > 0
            push!(errors, "Illegal character/s: [$(join(illegal_matches, ", "))] in $xi.")
        end
        if !isnothing(additional_illegal_strings)
            for s in additional_illegal_strings
                # s = additional_illegal_strings[1]
                if !isnothing(match(Regex(s), xi))
                    push!(errors, "Illegal string [$s] in $xi.")
                end
            end
        end
    end
    if length(errors) > 0
        if length(errors) > 1
            errors = "\n\t- " .* errors
            error(join(errors))
        else
            error(join(errors, "\n"))
        end
    end
    nothing
end

"""
    check(df::DataFrame, col::String)::Nothing

Validate that a DataFrame contains a specified column and that any
string values within that column satisfy the naming conventions enforced
by `check_illegal_strings()`.

The function first verifies that the requested column exists in the
DataFrame. If the column contains string values, all unique values are
validated to ensure they contain only permitted ASCII characters and do
not contain prohibited characters or patterns.

# Arguments

- `df::DataFrame`: DataFrame containing the column to validate.
- `col::String`: Name of the column to check.

# Validation Performed

1. Confirm that `col` exists in `df`.
2. If the column is string-valued:
   - Validate all unique values using `check_illegal_strings()`.
   - Verify that values contain only permitted characters.
   - Verify that values contain only ASCII characters.

# Returns

- `nothing` if validation succeeds.

# Throws

- An exception if `col` does not exist in `df`.
- An exception if a string value contains illegal characters.
- An exception if a string value contains non-ASCII characters.

# Notes

- Only string-valued columns are checked for illegal characters.
- Validation is performed on unique values only to reduce redundant
  checks and improve performance.
- Any validation error from `check_illegal_strings()` is rethrown with
  additional context identifying the offending column.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df = DataFrame(aye=["abc", "def", "ghi"], nay=["hello world ^_^", "wildcard*", "slash/slash"]);

julia> try isnothing(check(df, "aye")); catch; false; end
true

julia> try isnothing(check(df, "nay")); catch; false; end
false
```
"""
function check(df::DataFrame, col::String)
    if col∉names(df)
        error(
            "The \"$col\" column does not exist in the dataframe (Existing columns: [\"$(join(names(df), "\", \""))\"])!",
        )
    end
    if eltype(df[!, col]) <: AbstractString
        try
            check_illegal_strings(String.(unique(df[!, col])))
        catch e
            new_error = join(["Illegal string in the \"$col\" column!\n", sprint(showerror, e)])
            error(new_error)
        end
    end
    nothing
end

"""
    check(conn::LibPQ.Connection, table::String)::Nothing

Verify that a table exists in the connected PostgreSQL database.

The supplied table name is first validated using `check_illegal_strings()`
before querying the PostgreSQL system catalog.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL connection.
- `table::String`: Name of the table to check.

# Returns
- `nothing` if the table exists.

# Throws
- An exception if `table` contains illegal characters.
- An exception if the table does not exist.

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
    check(conn::LibPQ.Connection, table::String, field::String)::Nothing

Verify that a field exists within a database table. 

Both the table name and field name are validated using `check_illegal_strings()` before querying the PostgreSQL system catalog.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL connection.
- `table::String`: Name of the table.
- `field::String`: Name of the field to check. 

# Returns
- `nothing` if the field exists within the specified table. 

# Throws 
- An exception if `table` or `field` contain illegal characters. 
- An exception if the field does not exist in the specified table. 

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
    validate_trials(df::DataFrame)::Nothing

Validate that a DataFrame contains all required columns for the Trials structure.

Checks that the input DataFrame includes all mandatory fields from the Trials struct,
excluding columns that match the patterns "phenotypes" or "traits". Validates that string
columns contain only allowed characters using `check_illegal_strings()`. Raises an error
if any required columns are missing or contain illegal characters.

# Arguments
- `df::DataFrame`: The DataFrame to validate against the Trials structure requirements.

# Returns
- `Nothing`: Returns nothing if validation passes.

# Throws
- `String`: An error message listing missing columns if validation fails.
- `String`: An error message if any string column contains illegal characters or non-ASCII content.

# Details
1. All required columns derived from `fieldnames(Trials)` are present.
2. String-valued columns contain only permitted characters.
3. String-valued columns contain only ASCII characters. 
4. Most required columns are expected to contain string values.
The following columns are exceptions and may be stored as either string or numeric types:
   - `years`
   - `measurements`
   - `replications`
   - `blocks`
   - `rows`
   - `cols`
Any other required column with a non-string type is considered invalid.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> simulate_genomes() |> simulate_trials;

julia> df = load_trial_df("simulated_trials.tsv");

julia> isnothing(validate_trials(df))
true
```
"""
function validate_trials(df::DataFrame)::Nothing
    required_columns = sort(
        filter(x -> isnothing(match(Regex("phenotypes|traits"), x)), String.(string.(collect(fieldnames(Trials))))),
    )
    missing_columns = filter(x -> x∉names(df), required_columns)
    if length(missing_columns) > 0
        error("Missing columns: [\"$(join(missing_columns, "\", \""))\"].")
    end
    numeric_columns = String[]
    for x in required_columns
        # x = required_columns[end]
        if eltype(df[!, x]) <: AbstractString
            try
                check_illegal_strings(String.(unique(df[!, x])))
            catch e
                new_error = join(["Illegal string in the \"$x\" column!\n", sprint(showerror, e)])
                error(new_error)
            end
        else
            push!(numeric_columns, x)
        end
    end
    unexpected_numeric_columns =
        filter(x -> x∉["years", "measurements", "replications", "blocks", "rows", "cols"], numeric_columns)
    if length(unexpected_numeric_columns) > 0
        error("Unexpected numeric column/s: [\"$(join(unexpected_numeric_columns, "\", \""))\"]")
    end
    nothing
end

"""
    validate_date(date::String)::Bool

Validate that a date string follows the strict `yyyy-mm-dd` format.
Note that we also allow `yyyy-m-d`, i.e. single digits for the month and day.

# Arguments
- `date::String`: A date string to validate.

# Returns
- `Bool`: `true` if the date string is valid, `false` otherwise.

# Details
The function checks that:
- The date contains exactly 3 parts separated by `-`
- The year part has exactly 4 digits
- The month part has 1-2 digits
- The day part has 1-2 digits
- All parts can be parsed as integers

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> validate_date("2026-07-08") |> x -> isnothing(x)
true

julia> validate_date("2026-7-8") |> x -> isnothing(x)
true

julia> try validate_date("2026/07/08"); catch; false; end
false

julia> try validate_date("2026|07|08"); catch; false; end
false

julia> try validate_date("2026/JUL/08"); catch; false; end
false

julia> try validate_date("2026-July-8"); catch; false; end
false

julia> try validate_date("2026-02-30"); catch; false; end
false

```
"""
function validate_date(date::String)::Nothing
    date_split = split(date, '-')
    if (
        (length(date_split) != 3) ||
        (length(date_split[1]) != 4) ||
        ((length(date_split[2]) < 1) && (length(date_split[2]) > 2)) ||
        ((length(date_split[3]) < 1) && (length(date_split[3]) > 2)) ||
        sum(isnothing.(tryparse.(Int64, date_split))) > 0
    )
        error("Invalid date format: \"$date\". We expect \"yyyy-mm-dd\" format, where all values are integers.")
    end
    try
        Date(date, dateformat"yyyy-mm-dd")
    catch
        error("Invalid date: $(date)!")
    end
    nothing
end


"""
    validate_data_table(df::DataFrame)::Nothing

Validate that a data table conforms to the schema expected by
`query_table()` outputs and downstream analysis functions.

The function verifies that the supplied `DataFrame` contains the
required identifier and measurement fields expected for experimental
data. Fields ending in `_id` are treated equivalently to their
identifier-resolved counterparts by removing the `_id` suffix prior to
validation.

A valid table must contain all required fields except that exactly one
of `trait` or `environmental_variable` may be absent. This accommodates
both phenotype tables (which contain `trait`) and environmental tables
(which contain `environmental_variable`).

# Arguments

- `df::DataFrame`: Data table to validate.

# Validation Performed

1. Column names are normalised by removing any `_id` suffixes.
2. The following fields are expected:

   - `experiment`
   - `site`
   - `treatment`
   - `layout`
   - `measurement`
   - `entry`
   - `trait`
   - `environmental_variable`
   - `value`

3. All required fields must be present, except that one of the
   following may be missing:

   - `trait`
   - `environmental_variable`

# Returns

- `nothing` if validation succeeds.

# Throws

- An exception if required fields are missing.
- An exception if both `trait` and `environmental_variable` are missing.
- An exception if any required field other than `trait` or
  `environmental_variable` is missing.

# Notes

The function is designed to validate both phenotype and environmental
data tables:

- Phenotype tables typically contain a `trait` field.
- Environmental tables typically contain an
  `environmental_variable` field.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> df_okay_phe = DataFrame(experiment=[], site=[], treatment=[], layout=[], measurement=[], entry=[], trait=[], value=[]);

julia> df_okay_env = DataFrame(experiment=[], site=[], treatment=[], layout=[], measurement=[], entry=[], environmental_variable=[], value=[]);

julia> df_nope = DataFrame(experiment=[], site=[]);

julia> try isnothing(validate_data_table(df_okay_phe)); catch; false; end
true

julia> try isnothing(validate_data_table(df_okay_env)); catch; false; end
true

julia> try isnothing(validate_data_table(df_nope)); catch; false; end
false
```
"""
function validate_data_table(df::DataFrame)::Nothing
    # conn = dbconnect()
    # df = extract_table(conn, "phenotype_data")
    expected_fields = [
        "experiment",
        "site",
        "treatment",
        "layout",
        "measurement",
        "entry",
        "trait",
        "environmental_variable",
        "value",
    ]
    fields = replace.(names(df), Regex("_id\$") => "")
    missing_fields = filter(x -> x ∉ fields, expected_fields)
    okay = (
        (length(missing_fields) == 0) || (
            (length(missing_fields) == 1) && (missing_fields[1] == "trait") ||
            (missing_fields[1] == "environmental_variable")
        )
    )
    if !okay
        error("Unexpected field/s:\n\t- $(join(missing_fields, "\n\t- "))")
    end
    nothing
end

"""
    validate_filters(filters::Vector{Filter})::Nothing

Validate a collection of `Filter` objects.

The function verifies that all supplied filters reference the same
database table. This is required by functions such as `query_table()`,
which construct a single SQL query against a single table.

# Arguments

- `filters::Vector{Filter}`: Collection of filters to validate.

# Validation Performed

1. Extract the table name associated with each filter.
2. Confirm that exactly one unique table is represented across all
   filters.

# Returns

- `nothing` if validation succeeds.

# Throws

- An exception if the filters reference more than one table.

# Notes

A query can only target a single database table. Consequently, filter
collections such as:

```julia
[
    Filter(conn, table="phenotype_data", ...),
    Filter(conn, table="environment_data", ...)
]
```
are invalid and will raise an error.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table="phenotype_data", field="trait", filter_like="trait_"));

julia> push!(filters, Filter(conn, table="phenotype_data", field="value", filter_between=(10, 20)));

julia> push!(filters, Filter(conn, table="environment_data", field="value", filter_less_than=50));

julia> try isnothing(validate_filters(filters)); catch; false; end
false

julia> pop!(filters);

julia> try isnothing(validate_filters(filters)); catch; false; end
true
```
"""
function validate_filters(filters::Vector{Filter})::Nothing
    tables = unique([f.table for f in filters])
    if length(tables) > 1
        error("We expect one and only one table in the filters! See:\n\t- $(join(filters, "\n\t- "))")
    end
    nothing
end

"""
    list_tables(conn::LibPQ.Connection)::DataFrame

List all user tables in the connected PostgreSQL database.

The function queries PostgreSQL system statistics and returns the names
of all user tables together with their estimated row counts.

# Argument
- `conn::LibPQ.Connection`: Active PostgreSQL database connection.

# Returns
- `DataFrame`: A DataFrame containing:
  - `table_name::String`: Database table name.
  - `estimated_row_count::Integer`: PostgreSQL estimate of the
    number of rows in the table.

# Notes
- Row counts are obtained from PostgreSQL statistics
  (`pg_stat_user_tables`) and are therefore estimates rather than
  exact counts.
- Only user tables are returned.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> list_tables(conn) |> nrow > 0
true

julia> close(conn);
```
"""
function list_tables(conn::LibPQ.Connection)::DataFrame
    # conn = dbconnect()
    execute(
        conn,
        """
        SELECT 
            relname AS table_name, 
            n_live_tup AS estimated_row_count
        FROM 
            pg_stat_user_tables
        """,
    ) |> DataFrame |> sort
end

"""
    extract_table( conn::LibPQ.Connection, table::String, )::DataFrame

Extract all records from a database table.

Validates that the specified table exists using `check()` and then retrieves all rows and columns from the table.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `table::String`: Name of the table to extract.

# Returns
- `DataFrame`: A DataFrame containing all rows and columns from `table`.

# Throws
- An exception if `table` contains illegal characters.
- An exception if `table` does not exist in the database.

# Notes

For large tables, this function may require substantial memory because the entire table is loaded into a single `DataFrame`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> extract_table(conn, "entries") |> nrow > 0
true

julia> close(conn);
```
"""
function extract_table(conn::LibPQ.Connection, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"
    check(conn, table)
    execute(conn, "SELECT * FROM $table") |> DataFrame
end
