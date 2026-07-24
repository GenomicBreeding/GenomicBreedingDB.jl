"""
    upload_reference_genome!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
    )::Nothing

Upload a reference genome file to the database and register its metadata.

The function validates that the supplied file path is absolute and that the file
appears to be a valid reference genome FASTA file. Following validation, the
reference genome metadata are inserted into the `reference_genomes` table unless
the file has already been registered.

Before insertion, the database is queried to determine whether the supplied file
path already exists in the `reference_genomes` table. If exactly one matching
record is found, a warning containing the existing record information is emitted
and the function exits without modifying the database. An additional integrity
check ensures that multiple records cannot reference the same file path.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the reference genome FASTA file.
- `name::String`: Unique name used to identify the reference genome.
- `notes::String`: Descriptive notes associated with the reference genome.

# Returns

- `Nothing`: Reference genome metadata are inserted into the database, or no
  action is taken if the file has already been registered.

# Throws

- `ErrorException`: If the file path is not absolute.
- `ErrorException`: If the reference genome file does not exist.
- `ErrorException`: If the file does not appear to be a valid FASTA file.
- `ErrorException`: If multiple database records reference the same file path,
  indicating database corruption or an inconsistent database state.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted and the function returns immediately when the supplied
  file path has already been registered in the database.
- A warning is emitted when a reference genome with the same name already exists
  and the insert operation is ignored by
  `ON CONFLICT (name) DO NOTHING`.

# Notes

- Only absolute file paths are accepted.
- Reference genome validation is delegated to
  `check_reference_genome(fname)`.
- Existing registrations are identified using the stored `file_path` field.
- Duplicate file registrations are not permitted.
- Metadata are inserted into the `reference_genomes` table using the supplied
  name, file path, and notes.
- Existing records with the same name are preserved through the use of
  `ON CONFLICT (name) DO NOTHING`.
- The reference genome file itself is not stored in the database; only its
  metadata and file location are recorded.
- If an existing record is found for the supplied file path, information about
  that record is reported to assist with provenance tracking and debugging.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome);

julia> conn = dbconnect();

julia> upload_reference_genome!(conn, fname=abspath(fname_reference_genome), name=fname_reference_genome, notes="simulated");

julia> query_table(conn, filters=[Filter(conn, table="reference_genomes", field="name", filter_in=[fname_reference_genome])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_reference_genome!(conn::LibPQ.Connection; fname::String, name::String, notes::String)::Nothing
    # conn = dbconnect(); fname = string(pwd(), "/simulated_reference_genome-", Dates.now(), ".fa"); simulate_reference_genome(fname_reference_genome=fname); name = "Milnesium tardigradum"; notes = "Simulated reference genome";
    if !isabspath(fname)
        error("The path to the reference genome file is not absolute: \"$fname\"!")
    end
    check_reference_genome(fname)
    # Check if the file path has already been uploaded
    df_tmp = query_table(
        conn,
        filters = [Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname])],
    )
    if nrow(df_tmp) == 1
        info = string.(names(df_tmp), ": ", collect(df_tmp[1, :]))
        @warn string(
            "The reference genome \"$fname\" has already been uploaded with the following information:\n\t- ",
            join(info, "\n\t -"),
            ")",
        )
        return nothing
    end
    if nrow(df_tmp) > 1
        error("Catastropic error! We do not expect the same file (\"$fname\") to be in the database multiple times!")
    end
    res = execute(
        conn,
        """
        INSERT INTO reference_genomes
        (
            name,
            file_path,
            notes
        )
        VALUES (\$1,\$2,\$3)
        ON CONFLICT (name) DO NOTHING
        """,
        [name, fname, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the FASTA file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM reference_genomes") |> DataFrame
    nothing
end

"""
    upload_genotype_vcf!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
        fname_reference_genome::String,
    )::Nothing

Upload a genotype VCF file to the database and associate it with an existing
reference genome.

The function validates the supplied VCF file, verifies that its path is absolute,
confirms that the associated reference genome has already been registered in the
database, and then inserts a corresponding record into the `genotype_vcfs` table.

Validation is performed by inspecting the VCF header and confirming the presence
of the mandatory `#CHROM` line. Both uncompressed and gzip-compressed VCF files
are supported.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the genotype VCF file.
- `name::String`: Unique name used to identify the genotype dataset.
- `notes::String`: Descriptive notes associated with the genotype dataset.
- `fname_reference_genome::String`: Absolute path to a reference genome that has
  already been registered in the `reference_genomes` table.

# Returns

- `Nothing`: Metadata describing the genotype VCF dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the VCF file does not exist.
- `ErrorException`: If the VCF file path is not absolute.
- `ErrorException`: If the specified reference genome is not registered in the
  database.
- `ErrorException`: If the file does not appear to be a valid VCF file.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a record for the supplied VCF file already exists in
  the database.

# Notes

- The reference genome must be registered beforehand using
  `upload_reference_genome!`.
- Reference genome lookup is performed using the stored `file_path` field.
- Both plain-text and gzip-compressed VCF files are supported.
- VCF validation is based on detection of the mandatory `#CHROM` header line.
- The resolved reference genome identifier is stored in the
  `genotype_vcfs` table.
- Existing records are preserved through the use of
  `ON CONFLICT DO NOTHING`.
- The VCF file itself is not stored in the database; only its metadata, file
  path, and reference genome association are recorded.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_vcf = string("simulated_genotype_vcf-", Dates.now(),".vcf");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_vcf=fname_genomes_vcf);

julia> conn = dbconnect(); 

julia> try isnothing(upload_genotype_vcf!(conn, fname=abspath(fname_genomes_vcf), name=fname_genomes_vcf, notes="simulated", fname_reference_genome=abspath(fname_reference_genome))); catch; false; end
false

julia> upload_reference_genome!(conn, fname=abspath(fname_reference_genome), name=fname_reference_genome, notes="simulated");

julia> try isnothing(upload_genotype_vcf!(conn, fname=abspath(fname_genomes_vcf), name=fname_genomes_vcf, notes="simulated", fname_reference_genome=abspath(fname_reference_genome))); catch; false; end
true

julia> query_table(conn, filters=[Filter(conn, table="genotype_vcfs", field="name", filter_in=[fname_genomes_vcf])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_genotype_vcf!(
    conn::LibPQ.Connection;
    fname::String,
    name::String,
    notes::String,
    fname_reference_genome::String,
)::Nothing
    # conn = dbconnect(); fname_reference_genome = string(pwd(), "/simulated_reference_genome.fa"); fname = string(pwd(), "/simulated_genomes-", Dates.now(), ".vcf"); simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_vcf=fname); name = string("Simulated_VCF-", Dates.now()); notes = "Simulated reference genome";
    if !isabspath(fname)
        error("The path to the VCF file is not absolute: \"$fname\"!")
    end
    reference_genome_id = let
        df_reference_genome = query_table(
            conn,
            filters = [
                Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname_reference_genome]),
            ],
        )
        if nrow(df_reference_genome) == 0
            throw(
                "The reference genome file \"$fname\" is not found in the database. Please check the path or use `upload_reference_genome!(...)` first!",
            )
        end
        df_reference_genome.id[1]
    end
    check_vcf(fname)
    res = execute(
        conn,
        """
        INSERT INTO genotype_vcfs
        (
            name,
            file_path,
            reference_genome_id,
            notes
        )
        VALUES (\$1,\$2,\$3,\$4)
        ON CONFLICT DO NOTHING
        """,
        [name, fname, reference_genome_id, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the VCF file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM genotype_vcfs") |> DataFrame
    nothing
end

"""
    upload_genomes!(
        conn::LibPQ.Connection;
        fname::String,
        name::String,
        notes::String,
        fname_reference_genome::String,
    )::Nothing

Upload a `Genomes` dataset to the database and associate it with an existing
reference genome.

The function validates that the supplied file exists and appears to be a JLD2 file
containing a `Genomes` object, verifies that the file path is absolute, resolves
the associated reference genome from the database, and then registers the genomic
dataset in the `genomes` table.

The associated reference genome must already be registered in the database and is
identified using its stored file path. Existing genome records are preserved using
an `ON CONFLICT DO NOTHING` clause.

# Arguments

- `conn::LibPQ.Connection`: Active PostgreSQL database connection.
- `fname::String`: Absolute path to the JLD2 file containing a `Genomes`
  object.
- `name::String`: Unique name used to identify the genomic dataset.
- `notes::String`: Descriptive notes associated with the genomic dataset.
- `fname_reference_genome::String`: Absolute path to a reference genome already
  registered in the `reference_genomes` table.

# Returns

- `Nothing`: Metadata describing the genomic dataset are inserted into the
  database.

# Throws

- `ErrorException`: If the supplied file does not exist.
- `ErrorException`: If the file does not appear to contain a valid `Genomes`
  object.
- `ErrorException`: If the file path is not absolute.
- `ErrorException`: If the associated reference genome cannot be found in the
  database.
- Any database exception raised during insertion.

# Warnings

- A warning is emitted when a matching genome record already exists and the
  insert operation is ignored.

# Notes

- File validation is delegated to `check(Genomes; fname=...)`.
- Only absolute file paths are accepted.
- The associated reference genome must be registered beforehand using
  `upload_reference_genome!`.
- Reference genome lookup is performed using the `file_path` field of the
  `reference_genomes` table.
- The resolved reference genome identifier is stored in the
  `genomes.reference_genome_id` field.
- Records are inserted into the `genomes` table using the supplied name, file
  path, reference genome identifier, and notes.
- Existing records are preserved through the use of
  `ON CONFLICT DO NOTHING`.
- The JLD2 file itself is not stored in the database; only its metadata, file
  path, and reference genome association are recorded.
- This function is intended for registering previously generated genomic
  datasets rather than creating new ones.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> fname_genomes_jld2 = string("simulated_genotype_jld2-", Dates.now(),".jld2");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname_genomes_jld2);

julia> conn = dbconnect();

julia> try isnothing(upload_genomes!(conn, fname=abspath(fname_genomes_jld2), name=fname_genomes_jld2, notes="simulated", fname_reference_genome=abspath(fname_reference_genome))); catch; false; end
false

julia> upload_reference_genome!(conn, fname=abspath(fname_reference_genome), name=fname_reference_genome, notes="simulated");

julia> try isnothing(upload_genomes!(conn, fname=abspath(fname_genomes_jld2), name=fname_genomes_jld2, notes="simulated", fname_reference_genome=abspath(fname_reference_genome))); catch; false; end
true

julia> query_table(conn, filters=[Filter(conn, table="genomes", field="name", filter_in=[fname_genomes_jld2])]) |> nrow == 1
true

julia> close(conn);
```
"""
function upload_genomes!(
    conn::LibPQ.Connection;
    fname::String,
    name::String,
    notes::String,
    fname_reference_genome::String,
)::Nothing
    # conn = dbconnect(); fname_reference_genome = abspath("simulated_reference_genome.fa"); fname = string(pwd(), "/simulated_genomes-", Dates.now(), ".jld2"); simulate_genomes(fname_reference_genome=fname_reference_genome, fname_genomes_jld2=fname); name = replace(fname, ".jld2" => ""); notes = "Simulated genomes JLD2";
    check(Genomes, fname = fname)
    if !isabspath(fname)
        error("The path to the Genomes file is not absolute: \"$fname\"!")
    end
    reference_genome_id = let
        df_reference_genome = query_table(
            conn,
            filters = [
                Filter(conn, table = "reference_genomes", field = "file_path", filter_in = [fname_reference_genome]),
            ],
        )
        if nrow(df_reference_genome) == 0
            throw(
                "The reference genome file \"$fname\" is not found in the database. Please check the path or use `upload_reference_genome!(...)` first!",
            )
        end
        df_reference_genome.id[1]
    end
    res = execute(
        conn,
        """
        INSERT INTO genomes
        (
            name,
            file_path,
            reference_genome_id,
            notes
        )
        VALUES (\$1,\$2,\$3,\$4)
        ON CONFLICT DO NOTHING
        """,
        [name, fname, reference_genome_id, notes],
    )
    if LibPQ.num_affected_rows(res) == 0
        @warn "The record for the JLD2 file \"$fname\" already exists!"
    end
    # execute(conn, "SELECT * FROM genomes") |> DataFrame
    nothing
end
