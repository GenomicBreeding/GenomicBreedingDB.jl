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
        species_classification::Union{Missing, String}=missing,
        analysis::Union{Missing, String}=missing,
        analysis_description::Union{Missing, String}=missing,
        year::Union{Missing, Int64}=missing,
        season::Union{Missing, String}=missing,
        harvest::Union{Missing, String}=missing,
        site::Union{Missing, String}=missing,
        sep::String="\\t",
        verbose::Bool=false)::Nothing

Upload trial or phenotype data to a database from a delimited file or JLD2 format.

# Arguments
- `fname::String`: Path to the input file containing trial or phenotype data
- `species::String`: Species name (defaults to "unspecified")
- `species_classification::Union{Missing, String}`: Classification of the species
- `analysis::Union{Missing, String}`: Name of the analysis if applicable
- `analysis_description::Union{Missing, String}`: Description of the analysis
- `year::Union{Missing, Int64}`: Year of the trial/phenotype data
- `season::Union{Missing, String}`: Season of the trial/phenotype data
- `harvest::Union{Missing, String}`: Harvest identifier
- `site::Union{Missing, String}`: Site location
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

uploadtrialsorphenomes(fname=fname_trials, verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_1", verbose=true)
uploadtrialsorphenomes(fname=fname_trials, analysis="analysis_2", analysis_description="some description", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_3", verbose=true)
uploadtrialsorphenomes(fname=fname_phenomes, analysis="analysis_4", year=2030, season="Winter", verbose=true)

```
"""
function uploadtrialsorphenomes(;
    fname::String,
    species::String = "unspecified",
    species_classification::Union{Missing,String} = missing,
    analysis::Union{Missing,String} = missing,
    analysis_description::Union{Missing,String} = missing,
    year::Union{Missing,Int64} = missing,
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
    # species_classification = missing
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
                INSERT INTO entries (name, population, species, classification)
                VALUES (\$1, \$2, \$3, \$4)
                ON CONFLICT (name, population, species, classification) 
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            trait AS (
                INSERT INTO traits (name)
                VALUES (\$5)
                ON CONFLICT (name) 
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            trial AS (
                INSERT INTO trials (year, season, harvest, site)
                VALUES (\$6, \$7, \$8, \$9)
                ON CONFLICT (year, season, harvest, site)
                DO UPDATE SET description = EXCLUDED.description
                RETURNING id
            ),
            layout AS (
                INSERT INTO layouts (replication, block, row, col)
                VALUES (\$10, \$11, \$12, \$13)
                ON CONFLICT (replication, block, row, col)
                DO UPDATE SET replication = EXCLUDED.replication
                RETURNING id
            )
        INSERT INTO phenotype_data (entry_id, trait_id, trial_id, layout_id, value)
        SELECT entry.id, trait.id, trial.id, layout.id, \$14
        FROM entry, trait, trial, layout
        ON CONFLICT (entry_id, trait_id, trial_id, layout_id)
        DO NOTHING
    """
    expression_add_tag = if !ismissing(analysis)
        """
            WITH
                entry AS (
                    INSERT INTO entries (name, population, species, classification)
                    VALUES (\$1, \$2, \$3, \$4)
                    ON CONFLICT (name, population, species, classification) 
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                trait AS (
                    INSERT INTO traits (name)
                    VALUES (\$5)
                    ON CONFLICT (name) 
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                trial AS (
                    INSERT INTO trials (year, season, harvest, site)
                    VALUES (\$6, \$7, \$8, \$9)
                    ON CONFLICT (year, season, harvest, site)
                    DO UPDATE SET description = EXCLUDED.description
                    RETURNING id
                ),
                layout AS (
                    INSERT INTO layouts (replication, block, row, col)
                    VALUES (\$10, \$11, \$12, \$13)
                    ON CONFLICT (replication, block, row, col)
                    DO UPDATE SET replication = EXCLUDED.replication
                    RETURNING id
                ),
                analysis AS (
                    INSERT INTO analyses (name, description)
                    VALUES (\$14, \$15)
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
    pb = ProgressMeter.Progress(length(traits) * nrow(df), desc = "Uploading data: ")
    for trait in traits
        # trait = traits[1]
        for i = 1:nrow(df)
            # i = 1
            values = if isa(trials_or_phenomes, Trials)
                year_from_df = try
                    parse(Int64, df.years[i])
                catch
                    throw(
                        ArgumentError(
                            "The year in line $i, i.e. `$(df.years[i])` cannot be parsed into Int64.",
                        ),
                    )
                end
                [
                    df.entries[i],
                    df.populations[i],
                    species,
                    species_classification,
                    trait,
                    year_from_df,
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
                    df.populations[i],
                    species,
                    species_classification,
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
            execute(conn, expression, values)
            if !ismissing(analysis)
                execute(
                    conn,
                    expression_add_tag,
                    vcat(values[1:(end-1)], analysis, analysis_description),
                )
            end
            ProgressMeter.next!(pb)
        end
    end
    ProgressMeter.finish!(pb)
    # println("To commit please leave empty. To rollback enter any key:")
    # commit_else_rollback = readline()
    # if commit_else_rollback == ""
    #     execute(conn, "COMMIT;")
    # else
    #     execute(conn, "ROLLBACK;")
    # end
    close(conn)
end

# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO: uploadgenomes, uploadcvs, etc...
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
# TODO TODO TODO TODO TODO TODO TODO TODO TODO
