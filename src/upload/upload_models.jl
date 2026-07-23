"""
    upload_fit!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
    )::Nothing

Upload a fitted model dataset to the database and register its metadata.

The function validates that the supplied file exists and appears to be a JLD2 file
containing a `Fit` object, verifies that the file path is absolute, and then
registers the dataset in the `fits` table.

File validation is delegated to `check(Fit; fname=...)`, which performs a
lightweight inspection of the file to ensure it appears to contain a valid
`Fit` object. Existing records are preserved using an
`ON CONFLICT DO NOTHING` clause.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the JLD2 file containing a `Fit`
  object.
- `name::String`: Unique name used to identify the fitted model.
- `notes::String`: Descriptive notes associated with the fitted model.

# Returns

- `Nothing`: Metadata describing the fitted model are inserted into the
  database.

# Throws

- `ErrorException`: If the supplied file does not exist.
- `ErrorException`: If the file does not appear to contain a valid `Fit`
  object.
- `ErrorException`: If the file path is not absolute.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a matching fit record already exists and the
  insert operation is ignored.

# Notes

- File validation is delegated to `check(Fit; fname=...)`.
- Only absolute file paths are accepted.
- Records are inserted into the `fits` table using the supplied name, file
  path, and notes.
- Existing records are preserved through the use of
  `ON CONFLICT DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata and file
  location are recorded.
- This function is intended for registering previously generated fitted models
  for reproducibility, downstream prediction, and model evaluation workflows.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_fit_jld2 = string("simulated_fit_jld2-", Dates.now(),".jld2");

julia> genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> simulate_phenomes;

julia> simulate_fit(genomes, phenomes, fname_fit_jld2=fname_fit_jld2);

julia> conn = dbconnect(); 

julia> upload_fit!(conn, fname=abspath(fname_fit_jld2), name=fname_fit_jld2, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="fits", field="name", filter_in=[fname_fit_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_fit!(conn::LibPQ.Connection; fname::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname = string(pwd(), "/simulated_fit-", Dates.now(), ".jld2"); genomes = simulate_genomes(); phenomes = simulate_trials(genomes) |> simulate_phenomes; simulate_fit(genomes, phenomes, fname_fit_jld2=fname); name = replace(fname, ".tsv" => ""); notes = "simulated fit";
    check(Fit, fname = fname)
    if !isabspath(fname)
        error("The path to the Fit file is not absolute: \"$fname\"!")
    end
    res = execute(
        conn,
        """
        INSERT INTO fits
        (
            name,
            file_path,
            notes
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT DO NOTHING
        """,
        [name, fname, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM fits") |> DataFrame
    nothing
end
