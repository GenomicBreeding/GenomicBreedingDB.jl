function download_pedigrees(
    entries::Vector{String} = String[],
    species::Vector{String} = String[],
    entry_types::Vector{String} = String[],
    like_entries::Vector{String} = String[],
    like_species::Vector{String} = String[],
    like_entry_types::Vector{String} = String[],
    verbose::Bool = false,
)::DataFrame
    entries::Vector{String}=String[]
    species::Vector{String}=String[]
    entry_types::Vector{String}=String[]
    like_entries::Vector{String}=String["01", "02"]
    like_species::Vector{String}=String[]
    like_entry_types::Vector{String}=String[]
    verbose::Bool=true

    df_phenotype_data = download_phenotype_data(
        entries = entries,
        species = species,
        entry_types = entry_types,
        like_entries = like_entries,
        like_species = like_species,
        like_entry_types = like_entry_types,
        keep_id_and_do_not_unstack = false,
        verbose = verbose,
    )

    conn = dbconnect()

    extract_table_contents(conn, "entry_relationships")

    df_entries = query(
        conn,
        [Filter(conn, table = "entries", field = "name", filter_in = string.(unique(df_phenotype_data.entry)))],
        verbose = verbose,
    )
    df_children = query(
        conn,
        [Filter(conn, table = "entry_relationships", field = "child_id", filter_in = string.(unique(df_entries.id)))],
        verbose = verbose,
    )
    df_parents = query(
        conn,
        [Filter(conn, table = "entry_relationships", field = "parent_id", filter_in = string.(unique(df_entries.id)))],
        verbose = verbose,
    )
    df = vcat(df_children, df_parents)
    close(conn)
    df
end
