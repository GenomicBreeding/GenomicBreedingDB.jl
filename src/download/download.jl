function list_tables(conn::LibPQ.Connection)::DataFrame
    # conn = dbconnect()
    execute(
        conn, 
        """
        SELECT 
            relname AS table_name, 
            n_live_tup AS estimated_row_count
        FROM 
            pg_stat_user_tables
        """
    ) |> DataFrame |> sort
end

function exists(conn::LibPQ.Connection, table::String)::Bool
    # conn = dbconnect(); table = "rgsg"
    check_illegal_strings([table])
    execute(conn, "SELECT to_regclass('public.$table') IS NOT NULL AS table_exists") |> 
        DataFrame |> 
        x -> x.table_exists[1]
end

function extract_table(conn::LibPQ.Connection, table::String)::DataFrame
    # conn = dbconnect(); table = "entries"
    !exists(conn, table) ? error("The \"$table\" does not exist!") : nothing
    execute(conn, "SELECT * FROM $table") |>
        DataFrame
end

struct Filter
    table::String
    field::String
    in::Union{Nothing, Vector{String}, Vector{Int}, Vector{AbstractFloat}}
    between::Union{Nothing, Vector{Int}, Vector{AbstractFloat}}
    function Filter(;
        table::String,
        field::String,
        in_filter::Union{Nothing, Vector{String}, Vector{Int}, Vector{AbstractFloat}},
        between_filter::Union{Nothing, Vector{Int}, Vector{AbstractFloat}},
    )
        # table = "entries"; field = "name"; in = String["entry-1"]; between = nothing
        df_tables = list_tables(conn)
        if table ∉ df_tables.table_name
            error("The \"$table\" does not exist in the database!")
        end
        if isnothing(in_filter) && isnothing(between_filter)
            error("We expect either `in_filter` or `between_filter` to be defined.")
        end
        if !isnothing(in_filter) && !isnothing(between_filter)
            error("We expect only one of `in_filter` and `between_filter` to be defined.")
        end
        new(table, field, in_filter, between_filter)
    end
end

function query_table(
    conn::LibPQ.Connection;
    table::String,

)::DataFrame
    DataFrame()
end