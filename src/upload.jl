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



function test(;
    fname::String,
    species::String = "unspecified",
    ploidy::Union{Missing,String} = missing,
    crop_duration::Union{Missing,String} = missing,
    individual_or_pool::Union{Missing,String} = missing,
    maternal_family::Union{Missing,String} = missing,
    paternal_family::Union{Missing,String} = missing,
    cultivar::Union{Missing,String} = missing,
    analysis::Union{Missing, String} = missing,
    analysis_description::Union{Missing, String} = missing,
    year::Union{Missing, String} = missing,
    season::Union{Missing, String} = missing,
    harvest::Union{Missing, String} = missing,
    site::Union{Missing, String} = missing,
    sep::String = "\t",
    verbose::Bool = false,
)::Nothing
    # genomes = GenomicBreedingCore.simulategenomes(n=10, verbose=false);
    # trials, _ = GenomicBreedingCore.simulatetrials(genomes=genomes, verbose=false);
    # trials.years = replace.(trials.years, "year_" => "202")
    # fname = writedelimited(trials)
    # # tebv = analyse(trials, "y ~ 1|entries"); phenomes = merge(merge(tebv.phenomes[1], tebv.phenomes[2]), tebv.phenomes[3]); fname = writedelimited(phenomes)
    # species = "unspecified"
    # ploidy = missing
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
    # 1. READ & PREPARE DATA
    # Read the data file and determine its type.
    is_trials_data = false
    trials_or_phenomes = try
        trials_or_phenomes = try
            readdelimited(Trials, fname = fname, sep = sep, verbose = verbose)
        catch
            @suppress readjld2(Trials, fname = fname)
        end
        is_trials_data = true
        trials_or_phenomes
    catch
        trials_or_phenomes = try
            readdelimited(Phenomes, fname = fname, sep = sep, verbose = verbose)
        catch
            @suppress readjld2(Phenomes, fname = fname)
        end
        trials_or_phenomes
    end

    # Convert the wide data format into a long (tidy) format, ideal for bulk loading.
    df = tabularise(trials_or_phenomes)
    id_cols = is_trials_data ?
        [:id, :entries, :populations, :years, :seasons, :harvests, :sites, :replications, :blocks, :rows, :cols] :
        [:id, :entries, :populations]
    
    df_long = stack(df, Not(id_cols), variable_name=:trait)
    # df_long.value = Float32.(df_long.value)

    # 2. DATABASE UPLOAD
    # DotEnv.load!(joinpath(homedir(), ".env"))
    conn = dbconnect()
    try
        # Start a single transaction for the entire operation for speed and safety.
        execute(conn, "BEGIN;")
        
        if verbose println("Creating temporary staging table...") end
        # Create a temporary table that is dropped automatically when the transaction ends.
        execute(conn, """
            CREATE TEMP TABLE staging_data (
                entry_name TEXT,
                population TEXT,
                trait_name TEXT,
                year TEXT,
                season TEXT,
                harvest TEXT,
                site TEXT,
                replication TEXT,
                block TEXT,
                row_num TEXT,
                col_num TEXT,
                value DOUBLE PRECISION
            ) ON COMMIT DROP;
        """)

        # NOTE: The column order in `select` MUST match the order in `CREATE TEMP TABLE`.
        data_to_copy = select(df_long,
            :entries => :entry_name,
            :populations => :population,
            :trait => :trait_name,
            # Use data from the file if available, otherwise use function arguments
            (is_trials_data ? :years : (x -> year)) => :year,
            (is_trials_data ? :seasons : (x -> season)) => :season,
            (is_trials_data ? :harvests : (x -> harvest)) => :harvest,
            (is_trials_data ? :sites : (x -> site)) => :site,
            (is_trials_data ? :replications : (x -> missing)) => :replication,
            (is_trials_data ? :blocks : (x -> missing)) => :block,
            (is_trials_data ? :rows : (x -> missing)) => :row_num,
            (is_trials_data ? :cols : (x -> missing)) => :col_num,
            :value
        )
        
        if verbose println("Bulk-loading $(nrow(data_to_copy)) rows into staging table via COPY...") end
        # Execute the high-performance COPY command.
        for line in eachrow(data_to_copy)
            # line = data_to_copy[1, :]
            copyin = LibPQ.CopyIn("COPY staging_data FROM STDIN;", [join(Vector(line), "\t")])
            execute(conn, copyin)
        end

        if verbose println("Inserting data from staging table into final tables...") end
        
        # 3. UPSERT DATA FROM STAGING TABLE INTO FINAL TABLES (SET-BASED)
        # These queries run once on the entire dataset, not in a loop.
        
        # Upsert Entries
        execute(conn, """
            INSERT INTO entries (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar)
            SELECT DISTINCT entry_name, \$1, \$2, \$3, \$4, population, \$5, \$6, \$7 FROM staging_data
            ON CONFLICT (name, species, ploidy, crop_duration, individual_or_pool, population, maternal_family, paternal_family, cultivar) DO NOTHING;
        """, [species, ploidy, crop_duration, individual_or_pool, maternal_family, paternal_family, cultivar])

        # Upsert Traits, Trials, and Layouts
        execute(conn, "INSERT INTO traits (name) SELECT DISTINCT trait_name FROM staging_data ON CONFLICT (name) DO NOTHING;")
        execute(conn, "INSERT INTO trials (year, season, harvest, site) SELECT DISTINCT year, season, harvest, site FROM staging_data ON CONFLICT (year, season, harvest, site) DO NOTHING;")
        execute(conn, "INSERT INTO layouts (replication, block, row, col) SELECT DISTINCT replication, block, row_num, col_num FROM staging_data WHERE replication IS NOT NULL ON CONFLICT (replication, block, row, col) DO NOTHING;")

        # Finally, insert the phenotype data by joining all the dimension tables to get their IDs.
        if verbose println("Linking phenotype data...") end
        execute(conn, """
            INSERT INTO phenotype_data (entry_id, trait_id, trial_id, layout_id, value)
            SELECT
                e.id, t.id, tr.id, l.id, s.value
            FROM staging_data s
            JOIN entries e ON s.entry_name = e.name AND s.population = e.population -- Add other join conditions as needed
            JOIN traits t ON s.trait_name = t.name
            JOIN trials tr ON s.year = tr.year AND s.season = tr.season AND s.harvest = tr.harvest AND s.site = tr.site
            LEFT JOIN layouts l ON s.replication = l.replication AND s.block = l.block AND s.row_num = l.row AND s.col_num = l.col
            ON CONFLICT (entry_id, trait_id, trial_id, layout_id) DO NOTHING;
        """)

        # Handle analysis tags if provided.
        if !ismissing(analysis)
            if verbose println("Adding analysis tags...") end
            execute(conn, "INSERT INTO analyses (name, description) VALUES (\$1, \$2) ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description;", [analysis, analysis_description])
            execute(conn, """
                INSERT INTO analysis_tags (entry_id, trait_id, trial_id, layout_id, analysis_id)
                SELECT p.entry_id, p.trait_id, p.trial_id, p.layout_id, a.id
                FROM phenotype_data p, analyses a
                WHERE a.name = \$1
                ON CONFLICT (entry_id, trait_id, trial_id, layout_id, analysis_id) DO NOTHING;
            """, [analysis])
        end

        # If everything succeeded, commit the transaction.
        execute(conn, "COMMIT;")
        if verbose println("✅ Upload successful. Transaction committed.") end

    catch e
        # If any error occurs, roll back the entire transaction to ensure data integrity.
        println("❌ An error occurred. Rolling back transaction.")
        execute(conn, "ROLLBACK;")
        rethrow(e)
    finally
        # Always close the connection.
        close(conn)
    end
end




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
using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DotEnv
genomes = GenomicBreedingCore.simulategenomes(n=1_000, l=100, verbose=true)
trials, _ = GenomicBreedingCore.simulatetrials(genomes=genomes, f_add_dom_epi=rand(50, 3), proportion_of_variance=rand(9, 50), verbose=false);
trials.years = replace.(trials.years, "year_" => "203")
fname_trials = writedelimited(trials)
tebv = analyse(trials, "y ~ 1|entries")
phenomes = merge(merge(tebv.phenomes[1], tebv.phenomes[2]), tebv.phenomes[3])
fname_phenomes = writedelimited(phenomes)

dbinit()
DotEnv.load!(joinpath(homedir(), ".env"))
dbinit()
uploadtrialsorphenomes(fname=fname_trials, verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_1", verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_2", analysis_description="some description", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_3", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_4", year="2030-2031", ploidy="diploid", season="Winter", verbose=true)
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
