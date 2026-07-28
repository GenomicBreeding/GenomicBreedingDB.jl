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
    keep_id_and_do_not_unstack::Bool = false,
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
    # like_entry_types::Vector{String} = String["fam"]
    # like_traits::Vector{String} = String[]
    # replications::Vector{Int64} = Int64[]
    # blocks::Vector{Int64} = Int64[]
    # rows::Vector{Int64} = Int64[]
    # cols::Vector{Int64} = Int64[1]
    # keep_id_and_do_not_unstack::Bool = false
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
    fields_to_ignore = String["id", "name", "created_at", "updated_at"]
    fields_expected_phenotype_data =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "phenotype_data")), Regex("_id\$")=>"")
    fields_expected_entries =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "entries")), Regex("_id\$")=>"")
    fields_expected_layouts =
        replace.(filter(x -> x ∉ fields_to_ignore, extract_table_fields(conn, "layouts")), Regex("_id\$")=>"")
    # Query using phenotype_data filters only (we'll filter using entries and layouts later)
    if verbose
        println("(1/4) Querying using `phenotype_data` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[1]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
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
    if verbose
        println("(2/4) Querying using `entries` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[5]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
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
    rename!(df_entries, "name" => "entry")
    select!(df_entries, Not([:id, :notes, :created_at, :updated_at]))
    df = leftjoin(df_entries, df, on = :entry)
    # Filter df using layouts table filters
    if verbose
        println("(3/4) Querying using `layouts` table filters...")
    end
    filters = Filter[]
    for (k, v) in args
        # k = string.(keys(args))[5]; v = args[k]
        if length(v) == 0
            continue
        end
        is_like = !isnothing(match(Regex("^like_"), k))
        field =
            replace(k, Regex("ies\$")=>"y") |> x -> replace(x, Regex("s\$")=>"") |> x -> replace(x, Regex("^like_")=>"")
        if field ∉ fields_expected_layouts
            continue
        end
        if is_like
            for vi in v
                push!(filters, Filter(conn, table = "layouts", field = field, filter_like = vi))
            end
        else
            push!(filters, Filter(conn, table = "layouts", field = field, filter_in = v))
        end
    end
    df_layouts = query(conn, filters, verbose = verbose)
    rename!(df_layouts, "name" => "layout")
    select!(df_layouts, Not([:id, :created_at, :updated_at]))
    df = leftjoin(df_layouts, df, on = :layout)
    # Prepare the final dataframe
    if verbose
        println("(4/4) Preparing final table...")
    end
    df = if !keep_id_and_do_not_unstack
        df = unstack_data_table(df)
        select(df, Not("layout"))
    else
        df
    end
    close(conn)
    if verbose
        println("Done!")
    end
    df
end
