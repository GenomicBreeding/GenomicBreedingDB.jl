"""
    checkparams(params)::Nothing

Check input parameters for illegal characters that could be used in SQL injection attacks.

# Arguments
- `params`: Array or collection of parameters to check. Non-finite values (missing, NaN, Inf) are filtered out.

# Returns
- `Nothing`: Returns nothing if no illegal characters are found.

# Throws
- `ErrorException`: If illegal characters are found in any parameter, throws an error with details about which parameters
  contained illegal characters.

# Details
Checks for the following illegal characters:
- semicolon (;)
- hash (#)
- single quote (')
- double quote (")
- double dash (--)
- forward slash (\\)
- parentheses ( )
- equal sign (=)
- percent sign (%)
"""
function checkparams(params)::Nothing
    # params = ["kjshdf;", "kjhdsfk#", "sdkjfg'", "jdkfg\"", "dkjfg--", "dkjfg\\", "dkjfg(", "dkjfg)", "dkjfg=", "dkjfg%", "klsdhfpg_", "test-test"]
    params_filtered = []
    for x in params
        bool = try
            !ismissing(x) && !isnan(x) && !isinf(x)
        catch
            !ismissing(x)
        end
        if bool
            push!(params_filtered, x)
        end
    end
    illegal_chars = Dict(
        "semicolon" => ";"[1],
        "hash" => "#"[1],
        "single-quote" => "'"[1],
        "double-quote" => "\""[1],
        "forward-slash" => "\\"[1],
        "opening-parenthesis" => "("[1],
        "closing-parenthesis" => ")"[1],
        "equal-sign" => "="[1],
        "percent-sign" => "%"[1],
        "double-dash" => "--",
    )
    illegal_chars_found = Dict()
    for p in params_filtered
        # p = params_filtered[5]
        found = []
        for (k, v) in illegal_chars
            # k = "double-dash"; v = illegal_chars[k]
            if isa(v, Char) && sum(collect(p) .== v) > 0
                push!(found, k)
            elseif !isa(v, Char) && !isnothing(match(Regex(v), p))
                push!(found, k)
            end
        end
        if length(found) > 0
            illegal_chars_found[string(p)] = found
        end
    end
    if length(illegal_chars_found) > 0
        error("Illegal characters found in the following parameters:\n$(params)\ni.e.: $(illegal_chars_found).")
    end
    # Return nothing
    return nothing
end

"""
    querytable(
        table::String;
        fields::Union{Missing, Vector{String}} = missing,
        filters::Union{Missing, Dict{String, Tuple{Int64, Int64}}, Dict{String, Tuple{Float64, Float64}}, Dict{String, Vector{Int64}}, Dict{String, Vector{String}}} = missing,
    )::DataFrame

Query a PostgreSQL database table and return results as a DataFrame.

# Arguments
- `table::String`: Name of the database table to query
- `fields::Union{Missing, Vector{String}}`: Optional vector of column names to select. If missing, selects all columns (*)
- `filters::Union{Missing, Dict}`: Optional dictionary of filter conditions where:
    - Keys are column names (String)
    - Values can be:
        - `Tuple{Int64,Int64}` for BETWEEN conditions
        - `Vector{Int64}` for IN conditions with numbers
        - `Vector{String}` for IN conditions with strings

# Returns
- `DataFrame`: Query results as a DataFrame

# Examples
```julia
querytable("entries")
querytable("traits")
querytable("trials")
querytable("layouts")
querytable("phenotype_data")
querytable("analyses")
querytable("analysis_tags")

querytable("phenotype_data", fields=["id", "value"])
querytable("analyses", fields=["name", "description"])
querytable("trials", filters=Dict("year" => ["2021", "2030-2031"]))
querytable("layouts", filters=Dict("replication" => ["replication_1"]))
querytable("phenotype_data", fields=["id", "value"], filters=Dict("value" => (1.0, 10.0)))
```
"""
function querytable(
    table::String;
    fields::Union{Missing,Vector{String}} = missing,
    filters::Union{
        Missing,
        Dict{String,Tuple{Int64,Int64}},
        Dict{String,Tuple{Float64,Float64}},
        Dict{String,Vector{Int64}},
        Dict{String,Vector{String}},
    } = missing,
    like_filters::Union{Missing,Dict{String,Vector{String}}} = missing,
)::DataFrame
    conn = dbconnect()
    try
        # Check parameters
        checkparams([table])
        try
            execute(conn, "SELECT 1 FROM $table LIMIT 1")
        catch
            throw(ArgumentError("The table $table does not exist."))
        end
        colres = execute(
            conn,
            "SELECT column_name FROM information_schema.columns WHERE table_name = \$1",
            [table],
        )
        valid_cols = Set(row[1] for row in colres)
        if !ismissing(fields)
            checkparams(fields)
            for f in fields
                if !(f in valid_cols)
                    throw(ArgumentError("The column $f does not exist in table $table."))
                end
            end
        end
        if !ismissing(filters)
            checkparams(String.(keys(filters)))
            for k in keys(filters)
                if !(k in valid_cols)
                    throw(ArgumentError("The column $k does not exist in table $table."))
                end
            end
        end
        if !ismissing(like_filters)
            checkparams(String.(keys(like_filters)))
            for k in keys(like_filters)
                if !(k in valid_cols)
                    throw(ArgumentError("The column $k does not exist in table $table."))
                end
            end
        end
        # Build the query
        sql = if ismissing(fields)
            "SELECT * FROM $table"
        else
            "SELECT $(join(fields, ",")) FROM $table"
        end
        params = Any[]
        conds = String[]
        if !ismissing(filters)
            for (k, v) in filters
                if v isa Tuple
                    idx1 = length(params) + 1
                    idx2 = length(params) + 2
                    push!(conds, "$k BETWEEN \$$idx1 AND \$$idx2")
                    append!(params, v)
                elseif v isa Vector{Int64}
                    isempty(v) ? continue : nothing
                    inds = String[]
                    for val in v
                        push!(params, val)
                        push!(inds, "\$$(length(params))")
                    end
                    push!(conds, "$k IN ($(join(inds, ",")))")
                elseif v isa Vector{String}
                    isempty(v) ? continue : nothing
                    inds = String[]
                    for val in v
                        push!(params, val)
                        push!(inds, "\$$(length(params))")
                    end
                    push!(conds, "$k IN ($(join(inds, ",")))")
                end
            end
        end
        if !ismissing(like_filters)
            for (k, v) in like_filters
                isempty(v) ? continue : nothing
                inds = String[]
                for val in v
                    push!(params, "%$val%")
                    push!(inds, "($k LIKE \$$(length(params)))")
                end
                push!(conds, "( $(join(inds, " OR ")) )")
            end
        end
        sql = isempty(conds) ? sql : "$sql WHERE $(join(conds, " AND "))"
        res = isempty(params) ? execute(conn, sql) : execute(conn, sql, params)
        return DataFrame(res)
    catch e
        println("An error occurred! Rolling back transaction.")
        execute(conn, "ROLLBACK;")
        rethrow(e)
    finally
        close(conn)
    end
end

function databasesummary()::DataFrame
    conn = dbconnect()
    try
        schema = execute(conn, "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public'")
        results = DataFrame(
            table = String[],
            column = String[],
            type = String[],
            n = Int[],
            n_missing = Int[],
            min = Any[],
            max = Any[],
            mean = Any[],
            median = Any[],
            mode = Any[],
        )
        for row in schema
            # table, col, dtype = first(schema)
            table, col, dtype = row
            # Base counts
            n = first(execute(conn, "SELECT COUNT(*) FROM $table"))[1]
            n_missing = first(execute(conn,
                "SELECT COUNT(*) FROM $table WHERE $col IS NULL"
            ))[1]
            minv = maxv = meanv = medianv = modev = missing
            if n > 0
                # Numeric types
                if dtype ∈ ("integer", "bigint", "numeric", "real", "double precision")
                    stats = first(execute(conn,
                    """
                        SELECT
                            MIN($col),
                            MAX($col),
                            AVG($col),
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY $col)
                        FROM $table
                        WHERE $col IS NOT NULL
                    """))
                    minv, maxv, meanv, medianv = stats
                # String types
                elseif dtype ∈ ("uuid", "text", "character varying")
                    modev = first(execute(conn, """
                        SELECT $col
                        FROM $table
                        WHERE $col IS NOT NULL
                        GROUP BY $col
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    """))[1]
                end
            end
            push!(results, (
                table, col, dtype,
                n,
                n_missing,
                minv,
                maxv,
                meanv,
                medianv,
                modev
            ))
        end
        return results
    catch e
        println("An error occurred! Rolling back transaction.")
        execute(conn, "ROLLBACK;")
        rethrow(e)
    finally
        close(conn)
    end
end