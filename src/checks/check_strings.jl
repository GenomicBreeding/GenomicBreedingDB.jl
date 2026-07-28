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
