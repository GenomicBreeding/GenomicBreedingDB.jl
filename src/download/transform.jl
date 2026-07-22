"""
    unstack_data_table(df::DataFrame)::DataFrame

Convert a validated long-format data table into a wide-format `DataFrame`.

The function identifies all columns except `trait`, `environmental_variable`, and
`value` as row keys and uses either `trait` or `environmental_variable` as the
column key. The selected key column is expanded into individual columns using
`unstack`, with values sourced from the `value` column.

This transformation is useful for converting observational or measurement data from
a normalised long format into an analysis-ready wide format where traits or
environmental variables become separate columns.

# Arguments

- `df::DataFrame`: Long-format data table containing a `value` column and either a
  `trait` or `environmental_variable` column.

# Returns

- `DataFrame`: Wide-format table produced by unstacking the input data.

# Notes

- The input is validated using `validate_data_table` before reshaping.
- Exactly one of `trait` or `environmental_variable` is used as the column key.
- All remaining columns, excluding the key and value columns, are used to define
  unique rows in the output.
- Internally, the transformation is performed using `DataFrames.unstack`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="trait", filter_in=["trait_2", "trait_3"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 1_000)));

julia> df = query_table(conn, filters=filters);

julia> df_unstacked = unstack_data_table(df);

julia> ncol(df_unstacked) == (length(unique(df.trait)) + (ncol(df)-2))
true

julia> nrow(df_unstacked) < nrow(df)
true
```
"""
function unstack_data_table(df::DataFrame)::DataFrame
    # conn = dbconnect()
    # table = "phenotype_data"
    # filters = [
    #     Filter(conn, table=table, field="entry", filter_like="_01"),
    #     Filter(conn, table=table, field="trait", filter_in=["trait_2", "trait_3"]),
    #     Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]),
    #     Filter(conn, table=table, field="value", filter_between=(220, 250)),
    # ]
    # df = query_table(conn, table=table, filters=filters)
    validate_data_table(df)
    rowkeys = filter(x -> x∉["trait", "environmental_variable", "value"], names(df))
    colkey = filter(x -> x ∈ ["trait", "environmental_variable"], names(df))[1]
    unstack(df, rowkeys, colkey, "value")
end
