# Database interactions:
# 1. Upload new:
#   1.a. trials data
#   1.b. phenomes data
#   1.c. analysis information (including name and description)
#   1.d. TODO: reference genomes, and allele frequency data including genome coordinates
# 2. Update:
#   2.a. entries table with description
#   2.b. traits table with description
#   2.c. trials table with description
#   2.d. analyses table with additional analyses and/or tags on existing entry-trait-trial-layout combinations
#   2.e. TODO: genome marker variants table

"""
    uploadtrialsorphenomes(; fname::String, species::String="unspecified", 
        ploidy::Union{Missing,String}=missing,
        crop_duration::Union{Missing,String}=missing,
        individual_or_pool::Union{Missing,String}=missing,
        maternal_family::Union{Missing,String}=missing,
        paternal_family::Union{Missing,String}=missing,
        cultivar::Union{Missing,String}=missing,
        analysis::Union{Missing, String}=missing,
        analysis_description::Union{Missing, String}=missing,
        year::Union{Missing, String}=missing,
        season::Union{Missing, String}=missing,
        harvest::Union{Missing, String}=missing,
        site::Union{Missing, String}=missing,
        sep::String="\\t",
        verbose::Bool=false)::Nothing

Upload trial or phenotype data to a database from a delimited file or JLD2 format.

# Arguments
- `fname::String`: Path to the input file containing trial or phenotype data
- `species::String`: Species name (defaults to "unspecified")
- `ploidy::Union{Missing,String}`: Ploidy level of the species (e.g. diploid or tetraploid)
- `crop_duration::Union{Missing,String}`: Duration category of the crop (e.g. annual, short-term, or perennial)
- `individual_or_pool::Union{Missing,String}`: Whether entries are individuals or pools (e.g. individual, half-sib family, or synthetic population)
- `maternal_family::Union{Missing,String}`: Maternal family identifier
- `paternal_family::Union{Missing,String}`: Paternal family identifier 
- `cultivar::Union{Missing,String}`: Cultivar name
- `analysis::Union{Missing,String}`: Name of the analysis if applicable
- `analysis_description::Union{Missing,String}`: Description of the analysis
- `year::Union{Missing,String}`: Year of the trial/phenotype data which can be "2023-2024" for data with seasons spanning two years
- `season::Union{Missing,String}`: Season of the trial/phenotype data
- `harvest::Union{Missing,String}`: Harvest identifier
- `site::Union{Missing,String}`: Site location
- `sep::String`: Delimiter used in the input file (default is tab)
- `verbose::Bool`: Whether to print additional information during processing

# Description
Reads trial or phenotype data from a file and uploads it to a PostgreSQL database. The function can handle both
delimited text files and JLD2 format files. It performs the following operations:
1. Reads the input file as either Trials or Phenomes data
2. Inserts or updates entries in the database tables: entries, traits, trials, layouts
3. Records phenotype data with corresponding relationships
4. Optionally adds analysis tags if analysis information is provided

The function uses SQL transactions to ensure data integrity during the upload process.

# Returns
`Nothing`

# Note
Requires a properly configured database connection and appropriate table structure in the target database.

# Examples
```julia
genomes = GenomicBreedingCore.simulategenomes(n=10, verbose=false)
trials, _ = GenomicBreedingCore.simulatetrials(genomes=genomes, verbose=false)
trials.years = replace.(trials.years, "year_" => "202")
fname_trials = writedelimited(trials)
tebv = analyse(trials, "y ~ 1|entries")
phenomes = merge(merge(tebv.phenomes[1], tebv.phenomes[2]), tebv.phenomes[3])
fname_phenomes = writedelimited(phenomes)

dbinit()
DotEnv.load!(joinpath(homedir(), ".env"))
uploadtrialsorphenomes(fname=fname_trials, verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_1", verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_2", analysis_description="some description", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_3", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_4", year="2030-2031", season="Winter", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_4", ploidy="diploid", season="Summer", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_4", cultivar="mock_cultivar", season="Early Spring", verbose=true)
```
"""
function uploadtrialsorphenomes(;
    fname::String,
    species::String = "unspecified",
    ploidy::Union{Missing,String} = missing,
    crop_duration::Union{Missing,String} = missing,
    individual_or_pool::Union{Missing,String} = missing,
    maternal_family::Union{Missing,String} = missing,
    paternal_family::Union{Missing,String} = missing,
    cultivar::Union{Missing,String} = missing,
    analysis::Union{Missing,String} = missing,
    analysis_description::Union{Missing,String} = missing,
    year::Union{Missing,String} = missing,
    season::Union{Missing,String} = missing,
    harvest::Union{Missing,String} = missing,
    site::Union{Missing,String} = missing,
    sep::String = "\t",
    verbose::Bool = false,
)::Nothing
    # genomes = GenomicBreedingCore.simulategenomes(n=10, verbose=false);
    # trials, _ = GenomicBreedingCore.simulatetrials(genomes=genomes, verbose=false);
    # trials.years = replace.(trials.years, "year_" => "202")
    # fname = writedelimited(trials)
    # # tebv = analyse(trials, "y ~ 1|entries"); phenomes = merge(merge(tebv.phenomes[1], tebv.phenomes[2]), tebv.phenomes[3]); fname = writedelimited(phenomes)
    # species = "unspecified"
    # ploidy
    # crop_duration = missing
    # individual_or_pool = missing
    # maternal_family = missing
    # paternal_family = missing
    # cultivar = missing
    # analysis = missing
    # analysis_description = missing
    # year = missing
    # season = missing
    # harvest = missing
    # site = missing
    # sep = "\t"
    # verbose = true
    trials_or_phenomes = try
        try
            readdelimited(Trials, fname = fname, sep = sep, verbose = verbose)
        catch
            Suppressor.@suppress readjld2(Trials, fname = fname)
        end
    catch
        try
            readdelimited(Phenomes, fname = fname, sep = sep, verbose = verbose)
        catch
            Suppressor.@suppress readjld2(Phenomes, fname = fname)
        end
    end
    df = tabularise(trials_or_phenomes)
    expression = """
        WITH
            entry AS (
                INSERT INTO entries (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar)
                VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)
                ON CONFLICT (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar) 
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            trait AS (
                INSERT INTO traits (name)
                VALUES (\$10)
                ON CONFLICT (name) 
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            trial AS (
                INSERT INTO trials (year, season, harvest, site)
                VALUES (\$11, \$12, \$13, \$14)
                ON CONFLICT (year, season, harvest, site)
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            layout AS (
                INSERT INTO layouts (replication, block, row, col)
                VALUES (\$15, \$16, \$17, \$18)
                ON CONFLICT (replication, block, row, col)
                DO UPDATE SET replication = EXCLUDED.replication
                RETURNING id
            )
        INSERT INTO phenotype_data (entry_id, trait_id, trial_id, layout_id, value)
        SELECT entry.id, trait.id, trial.id, layout.id, \$19
        FROM entry, trait, trial, layout
        ON CONFLICT (entry_id, trait_id, trial_id, layout_id)
        DO NOTHING
    """
    expression_add_tag = if !ismissing(analysis)
        """
            WITH
                entry AS (
                    INSERT INTO entries (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar)
                    VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)
                    ON CONFLICT (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar) 
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                trait AS (
                    INSERT INTO traits (name)
                    VALUES (\$10)
                    ON CONFLICT (name) 
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                trial AS (
                    INSERT INTO trials (year, season, harvest, site)
                    VALUES (\$11, \$12, \$13, \$14)
                    ON CONFLICT (year, season, harvest, site)
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                layout AS (
                    INSERT INTO layouts (replication, block, row, col)
                    VALUES (\$15, \$16, \$17, \$18)
                    ON CONFLICT (replication, block, row, col)
                    DO UPDATE SET replication = EXCLUDED.replication
                    RETURNING id
                ),
                analysis AS (
                    INSERT INTO analyses (name, description)
                    VALUES (\$19, \$20)
                    ON CONFLICT (name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                )
            INSERT INTO analysis_tags (entry_id, trait_id, trial_id, layout_id, analysis_id)
            SELECT entry.id, trait.id, trial.id, layout.id, analysis.id
            FROM entry, trait, trial, layout, analysis
            ON CONFLICT (entry_id, trait_id, trial_id, layout_id, analysis_id)
            DO NOTHING
        """
    end
    conn = dbconnect()
    # execute(conn, "BEGIN;")
    traits = if isa(trials_or_phenomes, Trials)
        names(df)[12:end]
    else
        names(df)[4:end]
    end
    if verbose
        pb = ProgressMeter.Progress(length(traits) * nrow(df), desc = "Uploading data: ")
    end
    for trait in traits
        # trait = traits[1]
        for i = 1:nrow(df)
            # i = 1
            values = if isa(trials_or_phenomes, Trials)
                [
                    df.entries[i],
                    species,
                    ploidy,
                    crop_duration,
                    individual_or_pool,
                    df.populations[i],
                    maternal_family,
                    paternal_family,
                    cultivar,
                    trait,
                    df.years[i],
                    df.seasons[i],
                    df.harvests[i],
                    df.sites[i],
                    df.replications[i],
                    df.blocks[i],
                    df.rows[i],
                    df.cols[i],
                    df[i, trait],
                ]
            else
                [
                    df.entries[i],
                    species,
                    ploidy,
                    crop_duration,
                    individual_or_pool,
                    df.populations[i],
                    maternal_family,
                    paternal_family,
                    cultivar,
                    trait,
                    year,
                    season,
                    harvest,
                    site,
                    missing,
                    missing,
                    missing,
                    missing,
                    df[i, trait],
                ]
            end
            # println("EXPRESSION:")
            # println(expression)
            # println("VALUES:")
            # println(values)
            execute(conn, expression, values)
            if !ismissing(analysis)
                execute(
                    conn,
                    expression_add_tag,
                    vcat(values[1:(end-1)], analysis, analysis_description),
                )
            end
            if verbose
                ProgressMeter.next!(pb)
            end
        end
    end
    if verbose
        ProgressMeter.finish!(pb)
    end
    # println("To commit please leave empty. To rollback enter any key:")
    # commit_else_rollback = readline()
    # if commit_else_rollback == ""
    #     execute(conn, "COMMIT;")
    # else
    #     execute(conn, "ROLLBACK;")
    # end
    close(conn)
end


"""
    updatedescription(table::String;
        identifiers::Dict{String, Union{String, Missing}},
        description::String,
    )::Nothing

Update the description field in a specified table based on given identifiers.

# Arguments
- `table::String`: The name of the table to update.
- `identifiers::Dict{String, Union{String, Missing}}`: A dictionary of column names and their corresponding values to identify the rows to update. Missing values are allowed.
- `description::String`: The new description to set for the matching rows.

# Throws
- `ArgumentError`: If the table name or any identifier key/value contains a semicolon.
- `ArgumentError`: If the specified table or any identifier column does not exist in the database.

# Example
```julia
DotEnv.load!(joinpath(homedir(), ".env"))
querytable("entries")
updatedescription(
    "entries",
    identifiers = Dict(
        "name" => "entry_02",
        "species" => "unspecified",
        "population" => "pop_1",
        "classification" => missing,
    ),
    description = "Entry number 2 from population 1 with unspecified species and no additional classification details",
)
querytable("entries")
```
"""
function updatedescription(table::String;
    identifiers::Dict{String, Union{String, Missing}},
    description::String,
)::Nothing
    # table::String = "entries"
    # identifiers::Dict{String, Union{String, Missing}} = Dict(
    #     "name" => "entry_01",
    #     "species" => "unspecified",
    #     "population" => "pop_1",
    #     "classification" => missing,
    # )
    # description::String = "Entry number 1 from population 1 with unspecified species and no additional classification details"
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
    # Check if the identifiers are valid - part 1 of 2
    if table == "entries" 
        if sort(string.(keys(identifiers))) != sort(["name", "species", "population", "classification"])
            throw(ArgumentError("The identifiers for the table $table are not correct. You need to specify:\n\t‣ " * join(["name", "species", "population", "classification"], "\n\t‣ ")))
        end
    elseif table == "traits" 
        if sort(string.(keys(identifiers))) != sort(["name"])
            throw(ArgumentError("The identifiers for the table $table are not correct. You need to specify:\n\t‣ " * join(["name"], "\n\t‣ ")))
        end
    elseif table == "trials" 
        if sort(string.(keys(identifiers))) != sort(["year", "season", "harvest", "site"])
            throw(ArgumentError("The identifiers for the table $table are not correct. You need to specify:\n\t‣ " * join(["year", "season", "harvest", "site"], "\n\t‣ ")))
        end
    elseif table == "analyses" 
        if sort(string.(keys(identifiers))) != sort(["name"])
            throw(ArgumentError("The identifiers for the table $table are not correct. You need to specify:\n\t‣ " * join(["name"], "\n\t‣ ")))
        end
    else
        throw(ArgumentError("The table $table does not have a `description` field."))
    end
    # Check if the identifiers are valid - part 2 of 2
    for (k, v) in identifiers
        # k = string.(keys(identifiers))[1]; v = identifiers[k]
        if ismissing(v)
            continue
        end
        if !isnothing(match(Regex(";"), k) )
            throw(ArgumentError("The identifier key: '$k' cannot contain a semicolon."))
        end
        if !isnothing(match(Regex(";"), v) )
            throw(ArgumentError("The identifier value: '$v' cannot contain a semicolon."))
        end
        res = execute(
            conn,
            "SELECT $k FROM $table",
        )
        if length(res) == 0
            throw(ArgumentError("The column $k does not exist in table $table."))
        end
    end
    # Build the SQL query
    expression::Vector{String} = ["UPDATE $table SET description = \$1 WHERE"]
    parameters = [description]
    counter = [2]
    for (k, v) in identifiers
        # k = string.(keys(identifiers))[1]; v = identifiers[k]
        if ismissing(v)
            push!(expression, "($table.$k IS NULL) AND")
        else
            push!(expression, "($table.$k = \$$(counter[1])) AND")
            push!(parameters, v)
            counter[1] += 1
        end
    end
    # Remove the hanging 'AND'
    expression[end] = replace(expression[end], Regex(" AND\$") => "")
    # Update
    execute(conn, join(expression, " "), parameters)
    # Output
    nothing
end

# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO: uploadgenomes, uploadcvs, etc...
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
