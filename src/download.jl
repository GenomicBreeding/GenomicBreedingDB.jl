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
)::DataFrame
    # table = "entries"; fields = missing; filters = missing;
    # table = "phenotype_data"; fields=["id", "value"]; filters = missing;
    # table = "phenotype_data"; fields = missing; filters=Dict("value" => (100.0, 200.0));
    # Connect to the database
    conn = dbconnect()
    # Check arguments
    if !isnothing(match(Regex(";"), table))
        throw(ArgumentError("The table: '$table' cannot contain a semicolon."))
    end
    res = execute(
        conn,
        "SELECT table_name FROM information_schema.tables WHERE table_name = \$1",
        [table],
    )
    if length(res) == 0
        throw(ArgumentError("The table $table does not exist."))
    end
    if !ismissing(fields)
        for f in fields
            if !isnothing(match(Regex(";"), f) )
                throw(ArgumentError("The field: '$f' cannot contain a semicolon."))
            end    
            res = execute(
                conn,
                "SELECT \$1 FROM information_schema.columns WHERE table_name = \$2",
                [f, table],
            )
            if length(res) == 0
                throw(ArgumentError("The column $f does not exist in table $table."))
            end
        end
    end
    if !ismissing(filters)
        for (k, v) in filters
            if !isnothing(match(Regex(";"), k) )
                throw(ArgumentError("The filter key: '$k' cannot contain a semicolon."))
            end
            if isa(v, String) && !isnothing(match(Regex(";"), v) )
                throw(ArgumentError("The filter value: '$v' cannot contain a semicolon."))
            end
            res = execute(
                conn,
                "SELECT \$1 FROM information_schema.columns WHERE table_name = \$2",
                [string(k), table],
            )
            if length(res) == 0
                throw(
                    ArgumentError(
                        "The column $(string(k)) does not exist in table $table.",
                    ),
                )
            end
        end
    end
    # Build the expression
    expression_vector = ["SELECT"]
    if ismissing(fields)
        push!(expression_vector, "* FROM $table")
    else
        push!(expression_vector, string(join(fields, ","), " FROM $table"))
    end
    if ismissing(filters)
        nothing
    else
        push!(expression_vector, "WHERE")
        conditions::Vector{String} = []
        for (k, v) in filters
            if isa(v, Tuple{Int64,Int64}) || isa(v, Tuple{Float64,Float64})
                push!(conditions, string("(", k, " BETWEEN ", v[1], " AND ", v[2], ")"))
            elseif isa(v, Vector{Int64})
                push!(conditions, string("(", k, " IN (", join(v, ","), "))"))
            else
                # For Vector{String}
                # Notice the single-quotes between the string values
                push!(conditions, string("(", k, " IN ('", join(v, "','"), "'))"))
            end
        end
        push!(expression_vector, join(conditions, " AND "))
    end
    # Query
    res = execute(conn, join(expression_vector, " "))
    close(conn)
    # Output
    DataFrame(res)
end

"""
    extractmainfieldstablesandcols(conn::LibPQ.Connection)::DataFrame

Extract and sort main field tables and columns from a PostgreSQL database.

This function queries the information schema of a PostgreSQL database to get table and column names,
excluding system tables and specific columns/tables. It then sorts the fields in a predetermined order
by temporarily adding prefixes to certain column names.

# Arguments
- `conn::LibPQ.Connection`: Active PostgreSQL database connection

# Returns
- `DataFrame`: A sorted DataFrame with two columns:
  - `table_name`: Name of the database table
  - `column_name`: Name of the column in the corresponding table

# Details
The function:
1. Excludes system schemas ('pg_catalog', 'information_schema')
2. Excludes columns ending with 'id'
3. Excludes 'description' columns
4. Excludes 'traits' and 'phenotype_data' tables
5. Excludes tables matching 'analys%s%' pattern
6. Sorts fields consecutively: 
    6.1. species
    6.2. classification
    6.3. name
    6.4. population
    6.5. year
    6.6. season
    6.7. harvest
    6.8. site
    6.9. replication
    6.10. block
    6.11. row
    6.12. col
"""
function extractmainfieldstablesandcols(conn::LibPQ.Connection)::DataFrame
    tables = DataFrame(columntable(execute(
        conn,
        """
    SELECT 
        table_name,column_name 
    FROM information_schema.columns 
    WHERE 
        (table_schema NOT IN ('pg_catalog', 'information_schema')) AND
        (column_name NOT LIKE ('%id')) AND 
        (column_name NOT IN ('description')) AND
        (table_name NOT LIKE ('traits')) AND
        (table_name NOT IN ('phenotype_data')) AND
        (table_name NOT LIKE ('analys%s%'))
    ORDER BY table_name;
""",
    )))
    # Sort the field names sensibly
    tables.table_name[tables.table_name.=="layouts"] .= "z-layouts"
    tables.column_name[tables.column_name.=="replication"] .= "a-replication"
    tables.column_name[tables.column_name.=="block"] .= "b-block"
    tables.column_name[tables.column_name.=="row"] .= "c-row"
    tables.column_name[tables.column_name.=="col"] .= "d-col"
    tables.column_name[tables.column_name.=="species"] .= "a-species"
    tables.column_name[tables.column_name.=="classification"] .= "b-classification"
    tables.column_name[tables.column_name.=="name"] .= "c-name"
    tables.column_name[tables.column_name.=="population"] .= "d-population"
    tables.column_name[tables.column_name.=="year"] .= "e-year"
    tables.column_name[tables.column_name.=="season"] .= "f-season"
    tables.column_name[tables.column_name.=="harvest"] .= "g-harvest"
    tables.column_name[tables.column_name.=="site"] .= "h-site"
    sort!(tables)
    tables.table_name[tables.table_name.=="z-layouts"] .= "layouts"
    tables.column_name[tables.column_name.=="a-replication"] .= "replication"
    tables.column_name[tables.column_name.=="b-block"] .= "block"
    tables.column_name[tables.column_name.=="c-row"] .= "row"
    tables.column_name[tables.column_name.=="d-col"] .= "col"
    tables.column_name[tables.column_name.=="a-species"] .= "species"
    tables.column_name[tables.column_name.=="b-classification"] .= "classification"
    tables.column_name[tables.column_name.=="c-name"] .= "name"
    tables.column_name[tables.column_name.=="d-population"] .= "population"
    tables.column_name[tables.column_name.=="e-year"] .= "year"
    tables.column_name[tables.column_name.=="f-season"] .= "season"
    tables.column_name[tables.column_name.=="g-harvest"] .= "harvest"
    tables.column_name[tables.column_name.=="h-site"] .= "site"
    tables
end

"""
    addfilters!(expression::Vector{String}, counter::Vector{Int64}, parameters::Vector{Any}; 
                table::String, values::Union{Vector{String}, Vector{Float64}, Vector{Int64}])::Nothing

Add SQL `LIKE` filters to a query expression for pattern matching on table names.

This function modifies the input vectors to build SQL `LIKE` conditions for pattern matching,
replacing wildcards (*) with SQL wildcards (%) and handling multiple values.

# Arguments
- `expression::Vector{String}`: Vector to store SQL query expressions
- `counter::Vector{Int64}`: Vector containing a single counter for parameter numbering
- `parameters::Vector{Any}`: Vector to store parameter values for the query
- `table::String`: Name of the database table
- `values::Union{Vector{String}, Vector{Float64}, Vector{Int64}}`: Values to match against table names

# Effects
- Modifies `expression` by adding `LIKE` conditions
- Increments the counter in `counter`
- Adds values to `parameters`

# Returns
`Nothing`
"""
function addfilters!(
    expression::Vector{String},
    counter::Vector{Int64},
    parameters::Vector{Any};
    table::String,
    column::String,
    values::Union{
        Vector{Union{String, Missing}},
        Vector{Union{Float64, Missing}},
        Vector{Union{Int64, Missing}},
        Vector{String},
        Vector{Float64},
        Vector{Int64},
        Tuple{Float64,Float64},
        Tuple{Int64,Int64},
    },
)::Nothing
    # Check arguments
    if !isnothing(match(Regex(";"), table))
        throw(ArgumentError("The table: '$table' cannot contain a semicolon."))
    end
    if !isnothing(match(Regex(";"), column))
        throw(ArgumentError("The column: '$column' cannot contain a semicolon."))
    end
    # Build the expression
    push!(expression, "(")
    if isa(values, Vector{Union{String, Missing}}) || isa(values, Vector{String})
        for i in eachindex(values)
            # i = 2
            if !ismissing(values[i]) && !isnothing(match(Regex(";"), values[i]))
                throw(ArgumentError("The values[$i]: '$(values[i])' cannot contain a semicolon."))
            end
            if !ismissing(values[i])
                counter[1] += 1
                values[i] = replace(values[i], "*" => "%")
                push!(parameters, values[i])
                push!(expression, "($table.$column LIKE \$$(counter[1])) OR")
            else
                push!(expression, "($table.$column IS NULL) OR")
            end
        end
    elseif isa(values, Vector{Union{Float64, Missing}}) || isa(values, Vector{Union{Int64, Missing}}) || isa(values, Vector{Float64}) || isa(values, Vector{Int64})
        for i in eachindex(values)
            # i = 2
            if !ismissing(values[i])
                counter[1] += 1
                push!(parameters, values[i])
                push!(expression, "($table.$column = \$$(counter[1])) OR")
            else
                push!(expression, "($table.$column IS NULL) OR")
            end
        end
    else
        # Two-element tuple
        counter[1] += 2
        push!(parameters, minimum(values), maximum(values))
        push!(
            expression,
            "($table.$column BETWEEN \$$(counter[1]-1) AND \$$(counter[1])) OR",
        )
    end
    # Convert the hanging 'OR' to the closing parenthesis and 'AND' for the next filter
    expression[end] = replace(expression[end], Regex(" OR\$") => ") AND")
    # Output
    nothing
end

"""
    cleaunptraitnames(trait_name::String)::String

Clean up trait names by replacing whitespace and special characters with underscores.

# Arguments
- `trait_name::String`: The trait name to be cleaned

# Returns
- `String`: The cleaned trait name with spaces, tabs and pipe characters replaced with underscores
"""
function cleaunptraitnames(trait_name::String)::String
    trait_name = replace(trait_name, " " => "_")
    trait_name = replace(trait_name, "\t" => "_")
    trait_name = replace(trait_name, "|" => "_")
    trait_name
end

"""
    querytrialsandphenomes(;
        traits::Vector{String},
        species::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        classifications::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        populations::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        entries::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        years::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        seasons::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        harvests::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        sites::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        blocks::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        rows::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        cols::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        replications::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
        sort_rows::Bool = true,
        verbose::Bool = false,
    )::DataFrame

Query trials and phenotype data from a database with various filtering options.

# Arguments
- `traits`: Vector of trait names to query. Supports wildcards using "*".
- `species`: Optional vector of species names to filter by
- `classifications`: Optional vector of classification names to filter by
- `populations`: Optional vector of population names to filter by  
- `entries`: Optional vector of entry names to filter by
- `years`: Optional vector of years or tuple of (start_year, end_year) to filter by
- `seasons`: Optional vector of season names to filter by
- `harvests`: Optional vector of harvest names to filter by
- `sites`: Optional vector of site names to filter by
- `blocks`: Optional vector of block names to filter by
- `rows`: Optional vector of row numbers to filter by
- `cols`: Optional vector of column numbers to filter by
- `replications`: Optional vector of replication numbers to filter by
- `sort_rows`: Whether to sort the output rows (default: true)
- `verbose`: Whether to print progress messages (default: false)

# Returns
A DataFrame containing the queried trial and phenotype data with columns for all specified fields and traits.

# Examples
```julia
querytrialsandphenomes(traits = ["trait_1"], verbose=true)
querytrialsandphenomes(traits = ["trait_1"], classifications=["", missing], entries=["entry_01","entry_09"], verbose=true)
querytrialsandphenomes(traits = ["trait_1", "trait_3"], entries=["entry_06"], verbose=true)
querytrialsandphenomes(traits = ["trait_1", "trait_3"], entries=["entry_06", "entry_03"], seasons=["season_1", "season_4"], verbose=true)
querytrialsandphenomes(traits = ["trait_1", "trait_3"], entries=["entry_06", "entry_03"], seasons=["season_3"], years=["2021"], verbose=true)
querytrialsandphenomes(traits = ["trait_*"], entries=["*1*"], seasons=["Winter"], years=["2030-2031"], verbose=true)
```
"""
function querytrialsandphenomes(;
    traits::Vector{String},
    species::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    classifications::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    populations::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    entries::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    years::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    seasons::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    harvests::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    sites::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    blocks::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    rows::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    cols::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    replications::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing,
    sort_rows::Bool = true,
    verbose::Bool = false,
)::DataFrame
    # traits::Vector{String} = ["trait_1"];
    # species::Union{Missing, Vector{String}} = missing;
    # classifications::Union{Missing, Vector{String}} = missing;
    # populations::Union{Missing, Vector{String}} = missing;
    # entries::Union{Missing, Vector{String}} = missing;
    # years::Union{Missing,Vector{Union{String, Missing}}, Vector{String}} = missing;
    # seasons::Union{Missing, Vector{String}} = missing;
    # harvests::Union{Missing, Vector{String}} = missing;
    # sites::Union{Missing, Vector{String}} = missing;
    # blocks::Union{Missing, Vector{String}} = missing;
    # rows::Union{Missing, Vector{String}} = missing;
    # cols::Union{Missing, Vector{String}} = missing;
    # replications::Union{Missing, Vector{String}} = missing;
    # sort_rows::Bool = true;
    # verbose::Bool = true;
    # Check arguments
    # SELECT DISTINCT table_name FROM information_schema.columns ORDER BY table_name;
    # SELECT DISTINCT table_schema FROM information_schema.columns ORDER BY table_schema;
    # SELECT table_name,table_schema FROM information_schema.columns;
    # SELECT table_name,column_name FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
    # Connect to the database
    if verbose
        println("Connecting to the database...")
    end
    conn = dbconnect()
    # Set output fields
    if verbose
        println("Extracting and sorting the relevant table and column names...")
    end
    tables = extractmainfieldstablesandcols(conn)
    if verbose
        println("Defining the output fields...")
    end
    expression::Vector{String} =
        ["SELECT", join(string.(tables.table_name, ".", tables.column_name), ",\n")]
    # Set each trait as additional output field
    if verbose
        println("Defining the output trait fields...")
    end
    traits = begin
        # If there are wild-cards ("*"), then we include the most similar trait names
        matches::Vector{String} = []
        for trait in replace.(traits, "*" => "%")
            # trait = replace.(traits, "*" => "%")[1]
            if !isnothing(match(Regex(";"), trait) )
                throw(ArgumentError("The trait: '$trait' cannot contain a semicolon."))
            end
            # If there is no '%' in trait then 'LIKE' is equivalent to '='
            res = DataFrame(
                execute(conn, "SELECT name FROM traits WHERE name = \$1", [trait]),
            )
            if nrow(res) > 0
                matches = vcat(matches, res.name)
            end
        end
        unique(sort(matches))
    end
    for trait in traits
        expression[end] = expression[end] * ","
        push!(
            expression,
            "MAX(CASE WHEN traits.name = '$trait' THEN phenotype_data.value END) AS $(cleaunptraitnames(trait))",
        )
    end
    # Define and join the source tables
    if verbose
        println("Defining the source tables...")
    end
    push!(
        expression,
        join(
            [
                "FROM"
                "phenotype_data"
                "JOIN"
                "entries ON phenotype_data.entry_id = entries.id"
                "JOIN"
                "traits ON phenotype_data.trait_id = traits.id"
                "JOIN"
                "trials ON phenotype_data.trial_id = trials.id"
                "JOIN"
                "layouts ON phenotype_data.layout_id = layouts.id"
            ],
            "\n",
        ),
    )
    # println(join(expression, "\n"))
    # Define the filters
    if verbose
        println("Setting the filter expressions and their respective parameters...")
    end
    counter = [0]
    parameters = []
    if !ismissing(species) ||
       !ismissing(classifications) ||
       !ismissing(populations) ||
       !ismissing(entries) ||
       !ismissing(years) ||
       !ismissing(seasons) ||
       !ismissing(harvests) ||
       !ismissing(sites) ||
       !ismissing(blocks) ||
       !ismissing(rows) ||
       !ismissing(cols) ||
       !ismissing(replications)
        push!(expression, "WHERE")
        !ismissing(species) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "entries",
            column = "species",
            values = species,
        ) : nothing
        !ismissing(classifications) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "entries",
            column = "classification",
            values = classifications,
        ) : nothing
        !ismissing(populations) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "entries",
            column = "population",
            values = populations,
        ) : nothing
        !ismissing(entries) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "entries",
            column = "name",
            values = entries,
        ) : nothing
        !ismissing(years) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "trials",
            column = "year",
            values = years,
        ) : nothing
        !ismissing(seasons) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "trials",
            column = "season",
            values = seasons,
        ) : nothing
        !ismissing(harvests) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "trials",
            column = "harvest",
            values = harvests,
        ) : nothing
        !ismissing(sites) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "trials",
            column = "site",
            values = sites,
        ) : nothing
        !ismissing(replications) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "layouts",
            column = "replication",
            values = replications,
        ) : nothing
        !ismissing(blocks) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "layouts",
            column = "block",
            values = blocks,
        ) : nothing
        !ismissing(rows) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "layouts",
            column = "row",
            values = rows,
        ) : nothing
        !ismissing(cols) ?
        addfilters!(
            expression,
            counter,
            parameters,
            table = "layouts",
            column = "col",
            values = cols,
        ) : nothing
        # Remove the hanging 'AND'
        expression[end] = replace(expression[end], Regex(" AND\$") => "")
    end
    # Define the aggregation columns, i.e. all the fields except the trait fields
    if verbose
        println("Defining the aggregation fields...")
    end
    push!(
        expression,
        string(
            "GROUP BY ",
            join(string.(tables.table_name, ".", tables.column_name), ",\n"),
        ),
    )
    # Optional sorting
    if sort_rows
        if verbose
            println("Sorting the rows...")
        end
        push!(
            expression,
            "ORDER BY ",
            join(string.(tables.table_name, ".", tables.column_name), ",\n"),
        )
    end
    # Parameterised query
    if verbose
        println("Executing the parameterised query...")
        println("=========================================")
        println(join(expression, "\n"))
        println("=========================================")
    end
    res = if length(parameters) > 0
        execute(conn, join(expression, "\n"), parameters)
    else
        execute(conn, join(expression, "\n"))
    end
    # Close the database connection
    if verbose
        println("Closing the database connection...\nFinished!")
    end
    close(conn)
    # Output
    DataFrame(res)
end

"""
    queryanalyses(; analyses::Vector{String}, verbose::Bool = false)::DataFrame

Query and retrieve analysis data from a database, combining information from multiple tables and traits.

# Arguments
- `analyses::Vector{String}`: Vector of analysis names to query from the database
- `verbose::Bool=false`: If true, prints detailed progress information during execution

# Returns
- `DataFrame`: A DataFrame containing the combined analysis results with the following:
    - Standard fields from the main database tables
    - Dynamic columns for each trait associated with the requested analyses
    - Rows grouped by the main fields and aggregated trait values

# Details
The function performs the following operations:
1. Connects to the database
2. Extracts relevant table and column names
3. Builds a parameterized SQL query that:
   - Selects main fields from various tables
   - Pivots trait values into columns
   - Joins multiple tables (analysis_tags, phenotype_data, entries, traits, trials, layouts, analyses)
   - Filters results based on the requested analyses
   - Groups and sorts the results

# Example

```julia
querytable("analyses")
queryanalyses(analyses=["analysis_1"], verbose=true)
queryanalyses(analyses=["analysis_2"], verbose=true)
queryanalyses(analyses=["analysis_3"], verbose=true)
queryanalyses(analyses=["analysis_4"], verbose=true)
queryanalyses(analyses=["analysis_1", "analysis_4"], verbose=true)
queryanalyses(analyses=["analysis_3", "analysis_4"], verbose=true)
```
"""
function queryanalyses(;
    analyses::Union{Vector{Union{String, Missing}}, Vector{String}},
    sort_rows::Bool = true,
    verbose::Bool = false,
)::DataFrame
    # analyses = ["analysis_3", "analysis_4"]; sort_rows = true; verbose = true
    # Check arguments
    for a in analyses
        if !ismissing(a) && !isnothing(match(Regex(";"), a))
            throw(ArgumentError("The analysis: '$a' cannot contain a semicolon."))
        end
    end
    if verbose
        println("Connecting to the database...")
    end
    conn = dbconnect()
    # Set output fields
    if verbose
        println("Extracting and sorting the relevant table and column names...")
    end
    tables = extractmainfieldstablesandcols(conn)
    if verbose
        println("Defining the output fields...")
    end
    expression::Vector{String} =
        ["SELECT", join(string.(tables.table_name, ".", tables.column_name), ",\n")]
    # println(join(expression, "\n"))
    # Extract the traits associated with the requested analyses
    expression_traits = [
        "SELECT DISTINCT traits.name as trait_names",
        "FROM ",
        "traits",
        "JOIN",
        "analysis_tags ON traits.id = analysis_tags.trait_id",
        "JOIN",
        "analyses ON analysis_tags.analysis_id = analyses.id",
        "WHERE",
        "analyses.name IN (",
        join(["\$$i" for i in eachindex(analyses)], ","),
        ")",
        "ORDER BY traits.name",
    ]
    traits = DataFrame(execute(conn, join(expression_traits, "\n"), analyses)).trait_names
    for trait in traits
        # trait = traits[1]
        expression[end] = expression[end] * ","
        push!(
            expression,
            "MAX(CASE WHEN traits.name = '$trait' THEN phenotype_data.value END) AS $(cleaunptraitnames(trait))",
        )
    end
    # println(join(expression, "\n"))
    # Define and join the source tables
    if verbose
        println("Defining the source tables...")
    end
    expression = vcat(
        expression,
        [
            "FROM",
            "analysis_tags",
            "JOIN",
            "analyses ON analysis_tags.analysis_id = analyses.id",
            "JOIN",
            "phenotype_data ON analysis_tags.entry_id = phenotype_data.entry_id",
            "AND analysis_tags.trait_id = phenotype_data.trait_id",
            "AND analysis_tags.trial_id = phenotype_data.trial_id",
            "AND analysis_tags.layout_id = phenotype_data.layout_id",
            "JOIN",
            "entries ON analysis_tags.entry_id = entries.id",
            "JOIN",
            "traits ON analysis_tags.trait_id = traits.id -- Join traits based on analysis_tags",
            "JOIN",
            "trials ON analysis_tags.trial_id = trials.id",
            "JOIN",
            "layouts ON analysis_tags.layout_id = layouts.id",
        ]
    )
    # println(join(expression, "\n"))

    # Define the filters
    if verbose
        println("Setting the filter expression for analyses...")
    end
    push!(expression, "WHERE")
    push!(
        expression,
        string("analyses.name IN (", join(["\$$i" for i in eachindex(analyses)], ","), ")"),
    )
    # Define the aggregation columns, i.e. all the fields except the trait fields
    if verbose
        println("Defining the aggregation fields...")
    end
    push!(
        expression,
        string(
            "GROUP BY ",
            join(string.(tables.table_name, ".", tables.column_name), ",\n"),
        ),
    )
    # Optional sorting
    if sort_rows
        if verbose
            println("Sorting the rows...")
        end
        push!(
            expression,
            "ORDER BY ",
            join(string.(tables.table_name, ".", tables.column_name), ",\n"),
        )
    end
    # Parameterised query
    if verbose
        println("Executing the parameterised query...")
        println("=========================================")
        println(join(expression, "\n"))
        println("=========================================")
    end
    res = execute(conn, join(expression, "\n"), analyses)
    # Close the database connection
    if verbose
        println("Closing the database connection...\nFinished!")
    end
    close(conn)
    # Output
    DataFrame(res)
end

"""
    df_to_io(df)

Convert a DataFrame to a CSV-formatted byte vector.

Takes a DataFrame `df` and serializes it to a tab-delimited CSV format in memory.
Returns the contents as a Vector{UInt8}.

# Arguments
- `df`: The DataFrame to convert to CSV format

# Returns
- `Vector{UInt8}`: The CSV data as a byte vector
"""
function df_to_io(df)
    io = IOBuffer()
    CSV.write(io, df, delim='\t')
    take!(io)
end
