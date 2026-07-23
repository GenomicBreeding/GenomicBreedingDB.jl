"""
    check_illegal_strings(
        x::Vector{String};
        additional_illegal_strings::Union{Nothing,Vector{String}}=nothing,
    )::Nothing

Validate a collection of strings against a predefined set of prohibited
characters and patterns.

The function enforces a strict naming convention intended for identifiers and
metadata stored in the database. Each string is checked for non-ASCII characters,
a predefined set of illegal characters, and optionally a collection of
user-specified disallowed string patterns.

If one or more violations are detected, an informative error is raised describing
all invalid strings and the corresponding offending characters or patterns.

# Arguments

- `x::Vector{String}`: Collection of strings to validate.
- `additional_illegal_strings::Union{Nothing,Vector{String}}=nothing`: Optional
  list of additional string patterns that are not permitted.

# Returns

- `Nothing`: Returned when all supplied strings pass validation.

# Throws

- `ErrorException`: If `x` is empty.
- `ErrorException`: If one or more strings contain non-ASCII characters.
- `ErrorException`: If one or more strings contain prohibited characters.
- `ErrorException`: If one or more strings match a pattern listed in
  `additional_illegal_strings`.

# Notes

- Validation is intentionally restrictive to promote consistent naming and
  identifier conventions throughout the database.
- Non-ASCII characters are not permitted.
- Prohibited characters include punctuation and symbols such as:
  `;`, `|`, `,`, `.`, `/`, `\\`, `"`, `'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`,
  `%`, `^`, `&`, `*`, `(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`,
  and `?`.
- Additional prohibited patterns may be supplied using
  `additional_illegal_strings`.
- All detected validation errors are reported together whenever possible.

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
        df::DataFrame,
        col::String,
    )::Nothing

Validate that a DataFrame contains a specified column and, when applicable,
validate the string values within that column.

The function verifies that `col` exists in the supplied `DataFrame`. If the column
contains string values, all unique entries are validated using
`check_illegal_strings` to ensure they conform to the project's naming and
identifier conventions.

If validation fails, the underlying error is rethrown with additional context
identifying the offending column.

# Arguments

- `df::DataFrame`: DataFrame to validate.
- `col::String`: Name of the column that must exist and, if applicable, contain
  valid string values.

# Returns

- `Nothing`: Returned when the column exists and all validation checks pass.

# Throws

- `ErrorException`: If the specified column does not exist in the DataFrame.
- `ErrorException`: If the column contains invalid string values.
- Any exception raised by `check_illegal_strings`, wrapped with contextual
  information identifying the column being validated.

# Notes

- String validation is performed only when the column element type is a subtype
  of `AbstractString`.
- Unique string values are validated to avoid redundant checks.
- Validation of string content is delegated to `check_illegal_strings`.
- The function does not modify the input `DataFrame`.

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
        type::Type{T};
        fname::String,
    )::Nothing where {T<:AbstractGB}

Validate that a file exists and appears to be a JLD2 file containing an object of
the specified GenomicBreeding type.

The function first verifies that the supplied file exists and then performs a
lightweight validation by inspecting the beginning of the file for signatures
indicating that it is a Julia-generated HDF5-backed JLD2 file containing an
object whose type matches the supplied `AbstractGB` subtype.

Validation succeeds only when the file contents contain the strings `Julia`,
`HDF5`, and the name of the requested type. If any of these signatures are
absent, an error is raised.

# Type Parameters

- `T <: AbstractGB`: Expected GenomicBreeding object type stored in the JLD2
  file, such as `Genomes`, `Phenomes`, or `Fit`.

# Arguments

- `type::Type{T}`: Expected object type contained within the file.
- `fname::String`: Path to the JLD2 file to validate.

# Returns

- `Nothing`: Returned when the file exists and appears to contain an object of
  the specified type.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a JLD2 file containing an
  object of the specified type.
- Any exception raised while opening or reading the file.

# Notes

- File existence is verified before any content validation is performed.
- Validation is performed by reading the first 1,000 bytes of the file.
- The function checks for the presence of the strings:
  - `Julia`
  - `HDF5`
  - `string(type)`
- This is a lightweight heuristic validation and does not fully deserialize the
  object.
- The function is intended to quickly verify file compatibility before loading
  larger datasets.
- Typical supported types include `Genomes`, `Phenomes`, `Fit`, and other
  subtypes of `AbstractGB`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> genomes = simulate_genomes();

julia> phenomes = simulate_trials(genomes) |> simulate_phenomes;

julia> simulate_fit(genomes, phenomes);

julia> try isnothing(check(Genomes, fname="simulated_genomes.jld2")); catch; false; end
true

julia> try isnothing(check(Phenomes, fname="simulated_phenomes.jld2")); catch; false; end
true

julia> try isnothing(check(Fit, fname="simulated_fit.jld2")); catch; false; end
true

julia> try isnothing(check(Fit, fname="some_non_exitent_file.jld2")); catch; false; end
false
```
"""
function check(type::Type{T}; fname::String)::Nothing where {T<:AbstractGB}
    if !isfile(fname)
        error("The $type file: \"$fname\" does not exist!")
    end
    tmp = open(fname, "r") do io
        read(io, 1_000) |> String
    end
    if isnothing(match(Regex("Julia"), tmp)) ||
       isnothing(match(Regex("HDF5"), tmp)) ||
       isnothing(match(Regex(string(type)), tmp))
        error("The file \"$fname\" may not be a JLD2 file containing a $type struct!")
    end
    nothing
end

"""
    validate_trials(
        df::DataFrame,
    )::Nothing

Validate that a DataFrame conforms to the expected structure and content of a
trial dataset.

The function verifies that all required trial columns are present, validates the
contents of string-valued columns against the project's naming conventions, and
checks that only expected fields are stored using numeric data types.

Required columns are determined from the `Trials` structure definition, excluding
phenotype and trait matrices. String columns are validated using
`check_illegal_strings`, whilst numeric columns are checked against a predefined
list of permitted numeric fields.

# Arguments

- `df::DataFrame`: Trial dataset to validate.

# Returns

- `Nothing`: Returned when all validation checks pass successfully.

# Throws

- `ErrorException`: If one or more required columns are missing.
- `ErrorException`: If a string column contains illegal characters or strings.
- `ErrorException`: If unexpected numeric columns are detected.

# Notes

- Required columns are derived from the fields of the `Trials` structure.
- Fields containing `phenotypes` or `traits` are excluded from the required-column
  check.
- String-valued columns are validated using `check_illegal_strings`.
- Numeric columns are expected only for:
  `years`, `measurements`, `replications`, `blocks`, `rows`, and `cols`.
- Any additional numeric columns are treated as potential data-formatting errors
  and will cause validation to fail.
- The function performs validation only and does not modify the input
  `DataFrame`.

# Examples

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
    validate_date(
        date::String,
    )::Nothing

Validate that a string represents a valid date in `yyyy-mm-dd` format.

The function verifies that the supplied string conforms to the expected date
format and that each component can be parsed as an integer. It then attempts to
construct a `Date` object to confirm that the date is valid according to the
Gregorian calendar.

Both formatting errors and invalid calendar dates result in an exception being
raised.

# Arguments

- `date::String`: Date string to validate.

# Returns

- `Nothing`: Returned when the supplied date is valid.

# Throws

- `ErrorException`: If the date does not conform to the `yyyy-mm-dd` format.
- `ErrorException`: If any date component is not an integer.
- `ErrorException`: If the date is not a valid calendar date.

# Notes

- Dates must follow the format `yyyy-mm-dd`.
- The year component must contain four digits.
- Month and day components must contain one or two digits.
- Validation includes both format checking and calendar validation.
- Internally, the date is validated using
  `Date(date, dateformat"yyyy-mm-dd")`.
- The function performs validation only and does not return a `Date` object.

# Examples

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
    validate_data_table(
        df::DataFrame,
    )::Nothing

Validate that a DataFrame conforms to the expected structure of a phenotype or
environmental data table.

The function checks that the DataFrame contains the required fields needed for
long-format observational data. Field names ending in `_id` are normalised by
removing the suffix before validation, allowing both identifier-based and
name-based representations of the data.

The dataset must contain all core experimental design fields together with a
`value` field and exactly one of either `trait` or
`environmental_variable`.

# Arguments

- `df::DataFrame`: Data table to validate.

# Returns

- `Nothing`: Returned when the DataFrame satisfies the expected structure.

# Throws

- `ErrorException`: If required fields are missing from the DataFrame.

# Notes

- Identifier fields ending in `_id` are treated as equivalent to their
  corresponding name-based fields.
- Required fields include:
  `experiment`, `site`, `treatment`, `layout`, `measurement`, `entry`,
  and `value`.
- Exactly one of `trait` or `environmental_variable` may be omitted.
- The function supports both phenotype and environmental data tables.
- Validation is limited to field presence and does not verify data types or
  field contents.
- The function performs validation only and does not modify the input
  `DataFrame`.

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
    validate_filters(
        filters::Vector{Filter},
    )::Nothing

Validate that a collection of filters references exactly one database table.

The function examines all supplied `Filter` objects and verifies that they target
the same table. This constraint is required for operations that construct a single
SQL query from multiple filters, such as querying or updating records.

If filters referencing multiple tables are detected, an error is raised describing
the offending filter definitions.

# Arguments

- `filters::Vector{Filter}`: Collection of filters to validate.

# Returns

- `Nothing`: Returned when all filters reference the same table.

# Throws

- `ErrorException`: If the filters reference more than one database table.

# Notes

- The function does not validate individual filter contents.
- Validation is limited to ensuring table consistency across all filters.
- An empty filter collection is not explicitly checked and may require validation
  elsewhere in the workflow.
- This function is commonly used before constructing SQL queries from multiple
  filter conditions.
- The function performs validation only and does not modify the supplied filters.

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
    check_reference_genome(
        fname::String,
    )::Nothing

Validate that a reference genome file exists and appears to be a valid FASTA file.

The function verifies that the supplied file exists and performs a lightweight
validation of its contents by locating the first FASTA record and inspecting the
associated sequence. Validation succeeds when the sequence contains the canonical
DNA nucleotide bases `A`, `T`, `C`, and `G`.

Both uncompressed FASTA files and gzip-compressed FASTA files are supported.

# Arguments

- `fname::String`: Path to the reference genome FASTA file.

# Returns

- `Nothing`: Returned when the file exists and appears to contain valid
  FASTA-formatted DNA sequence data.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a valid FASTA file.
- Any exception raised whilst opening or reading the file.

# Notes

- File existence is verified before content validation is performed.
- Both plain-text FASTA files and gzip-compressed FASTA files are supported.
- Validation is based on inspection of the first detected FASTA record.
- The function searches for the first header line beginning with `>`.
- Sequence validation is performed by checking for the presence of the canonical
  DNA bases `A`, `T`, `C`, and `G`.
- This is a lightweight heuristic validation and does not fully parse the FASTA
  file.
- The function performs validation only and does not modify the file.

# Examples
```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome);

julia> check_reference_genome(fname_reference_genome) |> isnothing
true
````
"""
function check_reference_genome(fname::String)::Nothing
    if !isfile(fname)
        error("The reference genome file: \"$fname\" does not exist!")
    end
    line = String[""]
    try
        open(fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    catch
        open(CodecZlib.GzipDecompressorStream, fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    end
    if sum([x ∈ unique(collect(line[1])) for x in ['A', 'T', 'C', 'G']]) < 4
        error("The \"$fname\" may not be a fasta file!")
    end
    nothing
end

"""
    check_vcf(
        fname::String,
    )::Nothing

Validate that a file exists and appears to be a valid Variant Call Format (VCF)
file.

The function verifies that the supplied file exists and performs a lightweight
validation of its contents by searching for the mandatory VCF header line
beginning with `#CHROM`. Validation succeeds when this header line is detected in
either an uncompressed or gzip-compressed file.

Both plain-text and gzip-compressed VCF files are supported.

# Arguments

- `fname::String`: Path to the VCF file.

# Returns

- `Nothing`: Returned when the file exists and appears to contain valid
  VCF-formatted data.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a valid VCF file.
- Any exception raised whilst opening or reading the file.

# Notes

- File existence is verified before content validation is performed.
- Both plain-text and gzip-compressed VCF files are supported.
- Validation is based on detection of the mandatory `#CHROM` header line.
- The function scans through comment lines until either `#CHROM` is found or an
  invalid record structure is encountered.
- This is a lightweight heuristic validation and does not verify the complete
  correctness of the VCF file.
- The function performs validation only and does not modify the file.

# Examples
```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_genomes_vcf = string("simulated_genotype_vcf-", Dates.now(),".vcf");

julia> simulate_genomes(fname_genomes_vcf=fname_genomes_vcf);

julia> check_vcf(fname_genomes_vcf) |> isnothing
true
````
"""
function check_vcf(fname::String)::Nothing
    if !isfile(fname)
        error("The VCF file: \"$fname\" does not exist!")
    end
    line = [String[""]]
    open(fname, "r") do io
        while line[1][1] != "#CHROM"
            line[1] = split(readline(io), "\t")
            if collect(line[1][1])[1] != '#'
                break
            end
        end
    end
    if line[1][1] != "#CHROM"
        open(CodecZlib.GzipDecompressorStream, fname, "r") do io
            while line[1][1] != "#CHROM"
                line[1] = split(readline(io), "\t")
                if collect(line[1][1])[1] != '#'
                    break
                end
            end
        end
    end
    if line[1][1] != "#CHROM"
        error("The \"$fname\" may not be a VCF file!")
    end
    nothing
end
