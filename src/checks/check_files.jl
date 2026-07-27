"""
    check(
        type::Type{T};
        fname::String,
    )::Nothing where {T<:AbstractGB}

Validate that a file exists and appears to be a JLD2 file containing an object of
the specified GenomicBreeding type.

The function first verifies that the supplied file exists and then performs a
lightweight validation by inspecting the beginning of the file for signatures
indicating that it is a Julia-generated HDF5-backed JLD2 file containing an
object whose type matches the supplied `AbstractGB` subtype.

Validation succeeds only when the file contents contain the strings `Julia`,
`HDF5`, and the name of the requested type. If any of these signatures are
absent, an error is raised.

# Type Parameters

- `T <: AbstractGB`: Expected GenomicBreeding object type stored in the JLD2
  file, such as `Genomes`, `Phenomes`, or `Fit`.

# Arguments

- `type::Type{T}`: Expected object type contained within the file.
- `fname::String`: Path to the JLD2 file to validate.

# Returns

- `Nothing`: Returned when the file exists and appears to contain an object of
  the specified type.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a JLD2 file containing an
  object of the specified type.
- Any exception raised while opening or reading the file.

# Notes

- File existence is verified before any content validation is performed.
- Validation is performed by reading the first 1,000 bytes of the file.
- The function checks for the presence of the strings:
  - `Julia`
  - `HDF5`
  - `string(type)`
- This is a lightweight heuristic validation and does not fully deserialize the
  object.
- The function is intended to quickly verify file compatibility before loading
  larger datasets.
- Typical supported types include `Genomes`, `Phenomes`, `Fit`, and other
  subtypes of `AbstractGB`.

# Examples

```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> genomes = simulate_genomes();

julia> phenomes = simulate_trials(genomes) |> simulate_phenomes;

julia> simulate_fit(genomes, phenomes);

julia> try isnothing(check(Genomes, fname="simulated_genomes.jld2")); catch; false; end
true

julia> try isnothing(check(Phenomes, fname="simulated_phenomes.jld2")); catch; false; end
true

julia> try isnothing(check(Fit, fname="simulated_fit.jld2")); catch; false; end
true

julia> try isnothing(check(Fit, fname="some_non_exitent_file.jld2")); catch; false; end
false
```
"""
function check(type::Type{T}; fname::String)::Nothing where {T<:AbstractGB}
    if !isfile(fname)
        error("The $type file: \"$fname\" does not exist!")
    end
    tmp = open(fname, "r") do io
        read(io, 1_000) |> String
    end
    if isnothing(match(Regex("Julia"), tmp)) ||
       isnothing(match(Regex("HDF5"), tmp)) ||
       isnothing(match(Regex(string(type)), tmp))
        error("The file \"$fname\" may not be a JLD2 file containing a $type struct!")
    end
    nothing
end

"""
    check_reference_genome(
        fname::String,
    )::Nothing

Validate that a reference genome file exists and appears to be a valid FASTA file.

The function verifies that the supplied file exists and performs a lightweight
validation of its contents by locating the first FASTA record and inspecting the
associated sequence. Validation succeeds when the sequence contains the canonical
DNA nucleotide bases `A`, `T`, `C`, and `G`.

Both uncompressed FASTA files and gzip-compressed FASTA files are supported.

# Arguments

- `fname::String`: Path to the reference genome FASTA file.

# Returns

- `Nothing`: Returned when the file exists and appears to contain valid
  FASTA-formatted DNA sequence data.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a valid FASTA file.
- Any exception raised whilst opening or reading the file.

# Notes

- File existence is verified before content validation is performed.
- Both plain-text FASTA files and gzip-compressed FASTA files are supported.
- Validation is based on inspection of the first detected FASTA record.
- The function searches for the first header line beginning with `>`.
- Sequence validation is performed by checking for the presence of the canonical
  DNA bases `A`, `T`, `C`, and `G`.
- This is a lightweight heuristic validation and does not fully parse the FASTA
  file.
- The function performs validation only and does not modify the file.

# Examples
```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_reference_genome = string("simulated_reference_genome-", Dates.now(),".fa");

julia> simulate_genomes(fname_reference_genome=fname_reference_genome);

julia> check_reference_genome(fname_reference_genome) |> isnothing
true
````
"""
function check_reference_genome(fname::String)::Nothing
    if !isfile(fname)
        error("The reference genome file: \"$fname\" does not exist!")
    end
    line = String[""]
    try
        open(fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    catch
        open(CodecZlib.GzipDecompressorStream, fname, "r") do io
            line[1] = readline(io)
            while line[1][1] != '>'
                line[1] = readline(io)
            end
            line[1] = readline(io)
        end
    end
    if sum([x ∈ unique(collect(line[1])) for x in ['A', 'T', 'C', 'G']]) < 4
        error("The \"$fname\" may not be a fasta file!")
    end
    nothing
end

"""
    check_vcf(
        fname::String,
    )::Nothing

Validate that a file exists and appears to be a valid Variant Call Format (VCF)
file.

The function verifies that the supplied file exists and performs a lightweight
validation of its contents by searching for the mandatory VCF header line
beginning with `#CHROM`. Validation succeeds when this header line is detected in
either an uncompressed or gzip-compressed file.

Both plain-text and gzip-compressed VCF files are supported.

# Arguments

- `fname::String`: Path to the VCF file.

# Returns

- `Nothing`: Returned when the file exists and appears to contain valid
  VCF-formatted data.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to be a valid VCF file.
- Any exception raised whilst opening or reading the file.

# Notes

- File existence is verified before content validation is performed.
- Both plain-text and gzip-compressed VCF files are supported.
- Validation is based on detection of the mandatory `#CHROM` header line.
- The function scans through comment lines until either `#CHROM` is found or an
  invalid record structure is encountered.
- This is a lightweight heuristic validation and does not verify the complete
  correctness of the VCF file.
- The function performs validation only and does not modify the file.

# Examples
```jldoctest; setup=:(using GenomicBreedingCore, GenomicBreedingIO, GenomicBreedingDB, DataFrames, CSV, StatsBase, LibPQ, Dates)
julia> fname_genomes_vcf = string("simulated_genotype_vcf-", Dates.now(),".vcf");

julia> simulate_genomes(fname_genomes_vcf=fname_genomes_vcf);

julia> check_vcf(fname_genomes_vcf) |> isnothing
true
````
"""
function check_vcf(fname::String)::Nothing
    if !isfile(fname)
        error("The VCF file: \"$fname\" does not exist!")
    end
    line = [String[""]]
    open(fname, "r") do io
        while line[1][1] != "#CHROM"
            line[1] = split(readline(io), "\t")
            if collect(line[1][1])[1] != '#'
                break
            end
        end
    end
    if line[1][1] != "#CHROM"
        open(CodecZlib.GzipDecompressorStream, fname, "r") do io
            while line[1][1] != "#CHROM"
                line[1] = split(readline(io), "\t")
                if collect(line[1][1])[1] != '#'
                    break
                end
            end
        end
    end
    if line[1][1] != "#CHROM"
        error("The \"$fname\" may not be a VCF file!")
    end
    nothing
end

"""
    check_dsv(
        fname::String,
        delims::Vector{String}=["\\t", ",", ";", " "],
    )::Nothing

Validate that a file exists and appears to contain tabular data (DSV: delimiter-separated values).

The function performs a lightweight inspection of the supplied file by examining
the first ten lines and testing whether they can be consistently split into the
same number of columns using one of the supplied delimiters. Validation succeeds
when at least one delimiter produces a consistent multi-column structure across
all inspected lines.

This function is intended as a simple heuristic for detecting delimited text
files such as TSV, CSV, space-delimited, or semicolon-delimited tables.

# Arguments

- `fname::String`: Path to the file to validate.
- `delims::Vector{String}=["\\t", ",", ";", " "]`: Candidate delimiters to test
  when assessing whether the file contains tabular data.

# Returns

- `Nothing`: Returned when the file exists and appears to contain tabular data.

# Throws

- `ErrorException`: If the specified file does not exist.
- `ErrorException`: If the file does not appear to contain a valid tabular
  structure.
- Any exception raised whilst opening or reading the file.

# Notes

- File existence is verified before content validation is performed.
- Validation is based on inspection of the first ten lines of the file.
- A delimiter is considered valid when all inspected lines split into the same
  number of fields and that number exceeds one.
- Multiple candidate delimiters may be evaluated before validation succeeds.
- Supported delimiters include tab, comma, semicolon, and space by default.
- This is a lightweight heuristic validation and does not guarantee that the
  entire file is a correctly formatted table.
- Files containing non-tabular header sections may fail validation even if later
  lines contain valid tabular data.
- The function performs validation only and does not modify the file.

# Examples

"""
function check_dsv(fname::String, delims::Vector{String} = ["\t", ",", ";", " "])::Nothing
    # fname = "simulated_trials.tsv"; simulate_genomes() |> simulate_trials
    # # fname = "simulated_genomes.vcf"; simulate_genomes()
    # delims::Vector{String}=["\t", ",", ";", " "]
    if !isfile(fname)
        error("The file: \"$fname\" does not exist!")
    end
    lines = String[]
    open(fname, "r") do io
        for i = 1:10
            push!(lines, readline(io))
        end
    end
    for delim in delims
        # delim = delims[1]
        x = [length(split(line, delim)) for line in lines] |> unique
        if (length(x) == 1) && (x[1] > 1)
            return nothing
        end
    end
    error("The file: \"$fname\" may not be a table or has non-tabular header line/s!")
end
