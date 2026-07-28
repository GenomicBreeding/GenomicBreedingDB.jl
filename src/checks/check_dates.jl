"""
    check_date(
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
julia> check_date("2026-07-08") |> x -> isnothing(x)
true

julia> check_date("2026-7-8") |> x -> isnothing(x)
true

julia> try check_date("2026/07/08"); catch; false; end
false

julia> try check_date("2026|07|08"); catch; false; end
false

julia> try check_date("2026/JUL/08"); catch; false; end
false

julia> try check_date("2026-July-8"); catch; false; end
false

julia> try check_date("2026-02-30"); catch; false; end
false

```
"""
function check_date(date::String)::Nothing
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
