# Main function to accept any file type and upload it into the database
function upload(
    fname::String;
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""],
    species::Union{Nothing,String} = nothing,
    experiment::Union{Nothing,String} = nothing,
    treatment::Union{Nothing,String} = nothing,
    entry_type::Union{Nothing,String} = nothing,
    population_type::Union{Nothing,String} = nothing,
    relationship_type::Union{Nothing,String} = nothing,
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing,
    name::Union{Nothing,String} = nothing,
    notes::Union{Nothing,String} = nothing,
    fname_reference_genome::Union{Nothing,String} = nothing,
    verbose::Bool = false,
)::Nothing
    missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]
    species::Union{Nothing,String} = nothing
    experiment::Union{Nothing,String} = nothing
    treatment::Union{Nothing,String} = nothing
    entry_type::Union{Nothing,String} = nothing
    population_type::Union{Nothing,String} = nothing
    relationship_type::Union{Nothing,String} = nothing
    measurement_dates::Union{Nothing,Dict{String,String}} = nothing
    name::Union{Nothing,String} = nothing
    notes::Union{Nothing,String} = nothing
    fname_reference_genome::Union{Nothing,String} = nothing
    verbose::Bool = true

    # # Trial data upload
    # fname = abspath(string("simulated_trials-", Dates.now(), ".tsv"))
    # simulate_genomes() |> x -> simulate_trials(x, fname_trials_tsv = fname)
    # species::Union{Nothing,String} = "Zea mays"
    # experiment::Union{Nothing,String} = "sim-exp"
    # treatment::Union{Nothing,String} = "control"
    # entry_type::Union{Nothing,String} = "family"
    # population_type::Union{Nothing,String} = "cultivar"
    # relationship_type::Union{Nothing,String} = "member_of"
    # # Phenomes file upload
    # fname = abspath(string("simulated_phenomes-", Dates.now(), ".jld2"))
    # simulate_genomes() |> simulate_trials |> x -> simulate_phenomes(x, fname_phenomes_jld2 = fname)
    # name = isnothing(name) ? basename(fname) : name
    # notes = isnothing(notes) ? "simulated data" : notes
    # # Environmental data upload
    # fname = abspath(string("simulated_environments-", Dates.now(), ".tsv"))
    # simulate_genomes() |> simulate_trials |> x -> simulate_environments(x, fname_environments_tsv = fname)
    # experiment::Union{Nothing,String} = "sim-exp"
    # treatment::Union{Nothing,String} = "control"
    # # Upload reference genome
    # fname = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_reference_genome = fname)
    # name = isnothing(name) ? basename(fname) : name
    # notes = isnothing(notes) ? "simulated data" : notes
    # # Upload VCF
    # fname = abspath(string("simulated_genomes-", Dates.now(), ".vcf"))
    # fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_genomes_vcf = fname, fname_reference_genome = fname_reference_genome)
    # upload_reference_genome!(conn, fname = fname_reference_genome, name = isnothing(name) ? basename(fname_reference_genome) : name, notes = isnothing(notes) ? "simulated data" : notes)
    # name = isnothing(name) ? basename(fname) : name
    # notes = isnothing(notes) ? "simulated data" : notes
    # # Upload Genomes
    # fname = abspath(string("simulated_genomes-", Dates.now(), ".jld2"))
    # fname_reference_genome = abspath(string("simulated_reference_genome-", Dates.now(), ".fa"))
    # simulate_genomes(fname_genomes_jld2 = fname, fname_reference_genome = fname_reference_genome)
    # upload_reference_genome!(conn, fname = fname_reference_genome, name = isnothing(name) ? basename(fname_reference_genome) : name, notes = isnothing(notes) ? "simulated data" : notes)
    # name = isnothing(name) ? basename(fname) : name
    # notes = isnothing(notes) ? "simulated data" : notes
    # # Upload Fit
    # fname = abspath(string("simulated_fit-", Dates.now(), ".jld2"))
    # genomes = simulate_genomes()
    # phenomes = simulate_trials(genomes) |> simulate_phenomes
    # simulate_fit(genomes, phenomes, fname_fit_jld2 = fname)
    # name = isnothing(name) ? basename(fname) : name
    # notes = isnothing(notes) ? "simulated data" : notes




    if !isfile(fname)
        error("The input file: \"$fname\" does not exist!")
    end
    # Determine input file type
    if verbose
        println("Determining data type of \"$fname\"...")
    end
    data_type_checks = Dict(
        # Phenotype data
        "trial_data" => try
            !isnothing(readdelimited(Trials, fname = fname))
        catch
            false
        end,
        "Phenomes" => try
            isnothing(check(Phenomes, fname = fname))
        catch
            false
        end,
        # Environmental data
        "environmental_data" => try
            !isnothing(extract_environment_variables(load_environments_df(fname, missing_strings = missing_strings)))
        catch
            false
        end,
        # Genptype data
        "reference_genome" => try
            isnothing(check_reference_genome(fname))
        catch
            false
        end,
        "vcf" => try
            isnothing(check_vcf(fname))
        catch
            false
        end,
        "Genomes" => try
            isnothing(check(Genomes, fname = fname))
        catch
            false
        end,
        # Model data
        "Fit" => try
            isnothing(check(Fit, fname = fname))
        catch
            false
        end,
    )
    filter!(x -> x.second, data_type_checks)
    if length(data_type_checks) == 0
        error("Unable to determine the type of \"$fname\"! Please refer to #link to file formats..(TODO...)")
    end
    if sum(values(data_type_checks)) > 1
        error(
            string(
                "Multiple format matches for \"$fname\"!\n\t- \"",
                join(keys(filter(x -> x.second, data_type_checks)), "\"\n\t- \""),
                "\"",
            ),
        )
    end
    # Upload
    conn = dbconnect()
    data_type = String.(keys(data_type_checks))[1]
    if verbose
        println("Uploading $data_type from \"$fname\"...")
    end
    if data_type == "trial_data"
        upload_trial_data!(
            conn,
            fname = fname,
            missing_strings = missing_strings,
            species = species,
            experiment = experiment,
            treatment = treatment,
            entry_type = entry_type,
            population_type = population_type,
            relationship_type = relationship_type,
            measurement_dates = measurement_dates,
            verbose = verbose,
        )
    elseif data_type == "Phenomes"
        upload_phenomes!(conn, fname = fname, name = name, notes = notes)
    elseif data_type == "environmental_data"
        upload_environment_data!(
            conn,
            fname = fname,
            missing_strings = missing_strings,
            experiment = experiment,
            treatment = treatment,
            measurement_dates = measurement_dates,
            verbose = verbose,
        )
    elseif data_type == "reference_genome"
        upload_reference_genome!(conn, fname = fname, name = name, notes = notes)
    elseif data_type == "vcf"
        upload_genotype_vcf!(
            conn,
            fname = fname,
            name = name,
            notes = notes,
            fname_reference_genome = fname_reference_genome,
        )
    elseif data_type == "Genomes"
        upload_genomes!(
            conn;
            fname = fname,
            name = name,
            notes = notes,
            fname_reference_genome = fname_reference_genome,
        )
    elseif data_type == "Fit"
        # TODO
    else
    end

    close(conn)
    nothing
end
