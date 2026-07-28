function download_phenotype_data(;
    experiments::Vector{String} = String[],
    sites::Vector{String} = String[],
    treatments::Vector{String} = String[],
    measurements::Vector{String} = String[],
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    traits::Vector{String} = String[],
    like_experiments::Vector{String} = String[],
    like_sites::Vector{String} = String[],
    like_treatments::Vector{String} = String[],
    like_measurements::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    like_traits::Vector{String} = String[],
    replications::Vector{Int64} = Int64[],
    blocks::Vector{Int64} = Int64[],
    rows::Vector{Int64} = Int64[],
    cols::Vector{Int64} = Int64[],
    verbose::Bool = false,
)::DataFrame
    # experiments::Vector{String} = String[]
    # sites::Vector{String} = String[]
    # treatments::Vector{String} = String[]
    # measurements::Vector{String} = String[]
    # entries::Vector{String} = String[]
    # species::Vector{String} = String[]
    # entry_types::Vector{String} = String[]
    # traits::Vector{String} = String[]
    # like_experiments::Vector{String} = String[]
    # like_sites::Vector{String} = String[]
    # like_treatments::Vector{String} = String[]
    # like_measurements::Vector{String} = String[]
    # like_entries::Vector{String} = String[]
    # like_species::Vector{String} = String[]
    # like_entry_types::Vector{String} = String["pop"]
    # like_traits::Vector{String} = String[]
    # replications::Vector{Int64} = Int64[]
    # blocks::Vector{Int64} = Int64[]
    # rows::Vector{Int64} = Int64[]
    # cols::Vector{Int64} = Int64[1]
    # verbose::Bool = true

    args = Dict(
        "experiments" => experiments,
        "sites" => sites,
        "treatments" => treatments,
        "measurements" => measurements,
        "entries" => entries,
        "species" => species,
        "entry_types" => entry_types,
        "traits" => traits,
        "like_experiments" => like_experiments,
        "like_sites" => like_sites,
        "like_treatments" => like_treatments,
        "like_measurements" => like_measurements,
        "like_entries" => like_entries,
        "like_species" => like_species,
        "like_entry_types" => like_entry_types,
        "like_traits" => like_traits,
        "replications" => replications,
        "blocks" => blocks,
        "rows" => rows,
        "cols" => cols,
    )
    conn = dbconnect()
    fields_to_ignore = String["id", "created_at", "updated_at"]
    fields_expected_phenotype_data = replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "phenotype_data")), Regex("_id\$")=>"")
    fields_expected_entries = replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "entries")), Regex("_id\$")=>"")
    fields_expected_layouts = replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "layouts")), Regex("_id\$")=>"")
    # Query using phenotype_data filters only (we'll filter using entries and layouts later)
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field = replace(k, Regex("ies\$")=>"y") |> 
            x -> replace(x, Regex("s\$")=>"") |> 
            x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_phenotype_data
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "phenotype_data", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "phenotype_data", field = field, filter_in = v))
        end        
    end
    filters = if length(filters) == 0
        push!(filters, Filter(conn, table = "phenotype_data", field = "experiment", filter_like = "%"))
    else
        filters
    end
    df = query(conn, filters, verbose = verbose)

    # Filter df using entries table filters
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[5]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field = replace(k, Regex("ies\$")=>"y") |> 
            x -> replace(x, Regex("s\$")=>"") |> 
            x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_entries
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "entries", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "entries", field = field, filter_in = v))
        end        
    end
    df_entries = query(conn, filters, verbose = verbose)




    
    # TODO: do the filtering and also do the same for layouts and return the final dataframe

    





    unstackable = try isnothing(check(df)); catch; false; end
    df = if unstack && unstackable
        unstack_data_table(df)
    else
        df
    end



    close(conn)
    df
end
