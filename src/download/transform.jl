"""
    unstack_data_table(
        df::DataFrame,
    )::DataFrame

Convert a long-format phenotype or environmental data table into a wide-format
table.

The function reshapes a data table by converting values stored in either the
`trait` or `environmental_variable` column into separate output columns. All
remaining fields are treated as row identifiers and are used to define unique
records in the resulting wide-format table.

Prior to reshaping, common database bookkeeping fields (`id`, `created_at`, and
`updated_at`) are removed when present. Missing bookkeeping fields are ignored,
allowing the function to operate on both raw database query results and
previously processed DataFrames.

The function automatically determines whether the input represents phenotype or
environmental data by detecting the presence of either a `trait` or
`environmental_variable` column.

# Arguments

- `df::DataFrame`: Long-format data table containing a `value` column and either
  a `trait` or `environmental_variable` column.

# Returns

- `DataFrame`: Wide-format table where trait names or environmental-variable
  names become individual columns and observed values are stored in the
  corresponding cells.

# Throws

- `ErrorException`: If the input DataFrame fails validation.
- `BoundsError`: If neither a `trait` nor `environmental_variable` column is
  present.
- Any exception raised by `DataFrames.unstack`.

# Notes

- Input validation is performed using `check(df)`.
- The columns `id`, `created_at`, and `updated_at` are removed if present.
- Missing bookkeeping columns are silently ignored.
- The function expects a long-format table containing a `value` column.
- The column key is automatically inferred from the first matching field among:
  - `trait`
  - `environmental_variable`
- All remaining columns other than the column key and `value` are used as row
  identifiers.
- Phenotype data are typically converted from long format:
  `entry × trait × value`
  into wide format:
  `entry × trait_1 × trait_2 × ...`.
- Environmental data are typically converted from long format:
  `site × environmental_variable × value`
  into wide format:
  `site × variable_1 × variable_2 × ...`.
- The function is intended for transforming query results into formats suitable
  for statistical analysis, modelling, visualization, and export.
- The input DataFrame is modified in-place prior to unstacking because
  bookkeeping columns are removed using `select!`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> conn = dbconnect();

julia> table = "phenotype_data";

julia> filters = Filter[];

julia> push!(filters, Filter(conn, table=table, field="entry", filter_like="1"));

julia> push!(filters, Filter(conn, table=table, field="site", filter_in=["site_1", "site_2"]));

julia> push!(filters, Filter(conn, table=table, field="trait", filter_in=["trait_2", "trait_3"]));

julia> push!(filters, Filter(conn, table=table, field="value", filter_between=(10, 1_000)));

julia> df = query(conn, filters);

julia> df_unstacked = unstack_data_table(df);

julia> sum(.!isnothing.(match.(Regex("trait"), names(df)))) < sum(.!isnothing.(match.(Regex("trait"), names(df_unstacked))))
true

julia> close(conn);
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
    # df = query(conn, table=table, filters=filters)
    check(df)
    for f in [:id, :created_at, :updated_at]
        try
            select!(df, Not(f))
        catch
            nothing
        end
    end
    rowkeys = filter(x -> x∉["trait", "environmental_variable", "value"], names(df))
    colkey = filter(x -> x ∈ ["trait", "environmental_variable"], names(df))[1]
    unstack(df, rowkeys, colkey, "value")
end
