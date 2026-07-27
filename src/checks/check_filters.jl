"""
    check(
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

julia> try isnothing(check(filters)); catch; false; end
false

julia> pop!(filters);

julia> try isnothing(check(filters)); catch; false; end
true
```
"""
function check(filters::Vector{Filter})::Nothing
    tables = unique([f.table for f in filters])
    if length(tables) > 1
        error("We expect one and only one table in the filters! See:\n\t- $(join(filters, "\n\t- "))")
    end
    nothing
end
