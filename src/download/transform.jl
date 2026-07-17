
"""
    unstack_data_table(df::DataFrame)::DataFrame

Convert a long-format data table into a wide-format table.

The function reshapes a validated data table by spreading the
`trait` or `environmental_variable` column into multiple columns and
using the corresponding `value` entries as cell values.

Prior to reshaping, the input is validated using
`validate_data_table()`.

# Arguments

- `df::DataFrame`: Data table in long format.

# Returns

- `DataFrame`: A wide-format table produced by unstacking the
  `trait` or `environmental_variable` column.

# Transformation

The following columns are treated as row identifiers:

- `experiment`
- `site`
- `treatment`
- `layout`
- `measurement`
- `entry`

Any additional columns present in the input, other than
`trait`, `environmental_variable`, and `value`, are also retained as
row identifiers.

The column used to define the new wide-format variables is:

- `trait`, if present; otherwise
- `environmental_variable`.

The corresponding `value` column provides the values populating the
wide-format table.

# Throws

- Any exception raised by `validate_data_table()`.
- Any exception raised by `DataFrames.unstack()`.

# Notes

This function is intended for converting tables returned by
`query_table()` into a format more suitable for statistical analysis
and modelling.

Phenotype data:

```text
entry    trait      value
-----    --------   -----
A        yield      10.1
A        height     150
B        yield      11.3
B        height     145
```

becomes:

```text
entry    yield    height
-----    -----    ------
A        10.1     150
B        11.3     145
```

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="_01"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="trait", filter_in=["trait_2", "trait_3"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(220, 250)));

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
