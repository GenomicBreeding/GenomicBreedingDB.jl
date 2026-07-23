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
    # fname = abspath(string("simulated_trials-", Dates.now(), ".tsv")); simulate_genomes() |> x -> simulate_trials(x, fname_trials_tsv=fname)
    # missing_strings::Vector{String} = ["missing", "NA", "na", "N/A", "n/a", ""]
    # species::Union{Nothing,String} = nothing
    # experiment::Union{Nothing,String} = nothing
    # treatment::Union{Nothing,String} = nothing
    # entry_type::Union{Nothing,String} = nothing
    # population_type::Union{Nothing,String} = nothing
    # relationship_type::Union{Nothing,String} = nothing
    # measurement_dates::Union{Nothing,Dict{String,String}} = nothing
    # name::Union{Nothing,String} = nothing
    # notes::Union{Nothing,String} = nothing
    # fname_reference_genome::Union{Nothing,String} = nothing
    # verbose::Bool = false
    conn = dbconnect()
    # TODO: First determine input file type...

    is_trial_data = try
        !isnothing(readdelimited(Trials, fname = fname))
    catch
        false
    end
    is_Phenomes = try
        isnothing(check(Phenomes, fname = fname))
    catch
        false
    end
    is_environmental_data = try
        !isnothing(extract_environment_variables(load_environments_df(fname, missing_strings = missing_strings)))
    catch
        false
    end

    is_reference_genome = try
        isnothing(check_reference_genome(fname))
    catch
        false
    end
    is_vcf = try
        isnothing(check_vcf(fname))
    catch
        false
    end
    is_Genomes = try
        isnothing(check(Genomes, fname = fname))
    catch
        false
    end
    is_Fit = try
        isnothing(check(Fit, fname = fname))
    catch
        false
    end
    


    nothing
end
