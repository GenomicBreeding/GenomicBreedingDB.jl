"""
    check_illegal_strings(x::Vector{String}; additional_illegal_strings::Union{Nothing, Vector{String}}=nothing)::Nothing

Validates that a vector of strings contains only allowed characters for database identifiers and names.

This function performs opinionated validation to ensure consistent naming conventions for identifiers
and text fields uploaded to database tables (excluding notes fields).

# Arguments
- `x::Vector{String}`: Vector of strings to check for illegal characters and non-ASCII content
- `additional_illegal_strings::Union{Nothing, Vector{String}}`: Optional additional strings to treat as illegal

# Throws
- `String`: Error message listing all validation failures (non-ASCII characters or illegal characters found)

# Illegal Characters
- The following characters are not allowed:
`;`, `|`, `,`, `.`, `/`, `\\`, `\"`, `\'`, `` ` ``, `~`, `!`, `@`, `#`, `\$`, `%`, `^`, `&`, `*`, 
`(`, `)`, `+`, `=`, `{`, `}`, `[`, `]`, `:`, `<`, `>`, `?`
- Any non-ASCII characters are rejected.
- Users can supply additional strings considered illegal

# Returns
- `nothing` if all validation checks pass

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
String columns are validated to ensure they contain only allowed characters for database 
identifiers and names (see `check_illegal_strings()` for details on allowed characters).
The following columns are exempt from character validation: `replications`, `blocks`, `rows`, and `cols`.

# Example

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname = simulate_trial(fname_output="test.tsv");

julia> df = load_trial_df(fname); rm(fname);

julia> isnothing(validate_trials(df))
true
```
"""
function validate_trials(df::DataFrame)::Nothing
    required_columns = sort(
        filter(x -> isnothing(match(Regex("phenotypes|traits"), x)), String.(string.(collect(fieldnames(Trials))))),
    )
    if required_columns != sort(required_columns ∩ names(df))
        error("Missing columns: [\"$(join(setdiff(required_columns, names(df)), "\", \""))\"].")
    end
    for x in setdiff(required_columns, ["replications", "blocks", "rows", "cols"])
        # x = setdiff(required_columns, ["replications", "blocks", "rows", "cols"])[1]
        if eltype(df[!, x]) <: AbstractString
            try
                check_illegal_strings(String.(unique(df[!, x])))
            catch e
                new_error = join(["Illegal string in the \"$x\" column!\n", sprint(showerror, e)])
                error(new_error)
            end
        end
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
    nothing
end
