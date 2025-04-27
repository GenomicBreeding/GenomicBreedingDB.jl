-- PostgreSQL Schema for Genomic Breeding (Optimized for VCF and Performance)

-- Add extensions first
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for UUID instead of SERIAL IDs (2^128 possible UUIDs!)
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- fuzzy matching


-- Entries (e.g., breeding lines, populations, families, cultivars, etc.)
CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    species TEXT NOT NULL,
    population TEXT,
    classification TEXT,
    description TEXT
);
-- Add unique constraint making sure that the NULLs are not considered distinct since in SQL, a NULL is not equivalent to other NULLs.
ALTER TABLE entries ADD CONSTRAINT unique_entry_instance UNIQUE NULLS NOT DISTINCT (name, species, population, classification); 

-- Traits (e.g., yield, height)
CREATE TABLE IF NOT EXISTS traits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

-- Yield trials metadata
CREATE TABLE IF NOT EXISTS trials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    year INT,
    season TEXT,
    harvest TEXT,
    site TEXT,
    description TEXT
);
-- Add unique constraint making sure that the NULLs are not considered distinct since in SQL, a NULL is not equivalent to other NULLs.
ALTER TABLE trials ADD CONSTRAINT unique_trial_instance UNIQUE NULLS NOT DISTINCT (year, season, harvest, site);

-- Field/experiment layout metadata
CREATE TABLE IF NOT EXISTS layouts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    replication TEXT,
    block TEXT,
    row TEXT,
    col TEXT
);
-- Add unique constraint making sure that the NULLs are not considered distinct since in SQL, a NULL is not equivalent to other NULLs.
ALTER TABLE layouts ADD CONSTRAINT unique_layout_instance UNIQUE NULLS NOT DISTINCT (replication, block, row, col);

-- Analyses metadata
CREATE TABLE IF NOT EXISTS analyses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

-- Phenotype measurements
CREATE TABLE IF NOT EXISTS phenotype_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID REFERENCES entries(id),
    trait_id UUID REFERENCES traits(id),
    trial_id UUID REFERENCES trials(id),
    layout_id UUID REFERENCES layouts(id),
    value FLOAT,
    CHECK (value IS NULL OR value != 'NaN'::FLOAT) -- Make sure we have FLOAT OR NULL, but never NAN
);
-- Add unique constraint making sure that the NULLs are not considered distinct since in SQL, a NULL is not equivalent to other NULLs.
ALTER TABLE phenotype_data ADD CONSTRAINT unique_phenotype_measurement UNIQUE NULLS NOT DISTINCT (entry_id, trait_id, trial_id, layout_id);

-- Analysis tags, where each entry-trait-trial-layout combination may have multiple analyses associated with them
CREATE TABLE IF NOT EXISTS analysis_tags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id UUID REFERENCES analyses(id),
    entry_id UUID REFERENCES entries(id),
    trait_id UUID REFERENCES traits(id),
    trial_id UUID REFERENCES trials(id),
    layout_id UUID REFERENCES layouts(id)
);
-- Add unique constraint making sure that the NULLs are not considered distinct since in SQL, a NULL is not equivalent to other NULLs.
ALTER TABLE analysis_tags ADD CONSTRAINT unique_analysis_instance UNIQUE NULLS NOT DISTINCT (analysis_id, entry_id, trait_id, trial_id, layout_id);

-- TODO: GENOTYPE DATA
-- -- Reference genomes
-- CREATE TABLE IF NOT EXISTS reference_genomes (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     name TEXT UNIQUE NOT NULL,
--     description TEXT
-- );
-- Variants (VCF-style markers)
-- CREATE TABLE IF NOT EXISTS variants (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     reference_genome_id UUID REFERENCES reference_genomes(id),
--     chrom TEXT,
--     pos INT,
--     ref TEXT,
--     alt TEXT,
--     name TEXT,
--     UNIQUE(reference_genome_id, chrom, pos)
-- );

-- -- Genotype matrix (entry × variant, VCF-style data)
-- CREATE TABLE IF NOT EXISTS genotype_data (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     entry_id UUID REFERENCES entries(id),
--     variant_id UUID REFERENCES variants(id),
--     genotype TEXT,         -- e.g., '0/1'
--     phred_quality REAL,    -- Optional: VCF QUAL field
--     depth UUID,             -- Optional: VCF DP field
--     UNIQUE(entry_id, variant_id)
-- );

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_phenotype_entry_trait_trial_layout ON phenotype_data (entry_id, trait_id, trial_id, layout_id);
CREATE INDEX IF NOT EXISTS idx_analysis_tag_lookup ON analysis_tags (analysis_id, entry_id, trait_id, trial_id, layout_id);

-- -- Create a GIN index for fuzzy search lookup performance on entry, site and trait names
-- CREATE INDEX trgm_entries_name_idx ON entries USING GIN (name gin_trgm_ops);
-- CREATE INDEX trgm_trials_site_idx ON trials USING GIN (site gin_trgm_ops);
-- CREATE INDEX trgm_traits_name_idx ON traits USING GIN (name gin_trgm_ops);