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
    # First determine input file type...
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

    conn = dbconnect()
    data_type = String.(keys(data_type_checks))[1]
    if data_type == "trial_data"
        # TODO
    elseif data_type == "Phenomes"
        # TODO
    elseif data_type == "environmental_data"
        # TODO
    elseif data_type == "reference_genome"
        # TODO
    elseif data_type == "vcf"
        # TODO
    elseif data_type == "Genomes"
        # TODO
    elseif data_type == "Fit"
        # TODO
    else
    end


    nothing
end
