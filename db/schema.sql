-- PostgreSQL Schema for GenomicBreedingDB.jl

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Notes
-- All references to id columns are UUIDs and named with the "_id" suffix (e.g. species_id, entries_id, trait_id), which are generated using the gen_random_uuid() function from the pgcrypto extension. 

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Entry type
CREATE TYPE entry_type AS ENUM (
    'cultivar',
    'population',
    'individual',
    'family',
    'not_set_yet'
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Relationship types, where relationships can be one-ot-one and one-to-many, e.g.:
--      - 'entry_1' member_of 'pop_A'
--      - 'entry_2' parent_is 'pop_A' AND 'entry_2' parent_is 'pop_B' AND 'entry_2' parent_is 'pop_C'
CREATE TYPE relationship_type AS ENUM (
    'member_of',
    'clone_of',
    'parent_is',
    'maternal_parent_is',
    'paternal_parent_is',
    'not_set_yet'
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Species
CREATE TABLE IF NOT EXISTS species (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    ploidy INT NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Entries table
--      - Entry names are unique across all species, i.e. no two entries can have the same name even if they are different species)
--      - Includes cultivars, populations, individuals, and families
CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    species_id UUID REFERENCES species(id) ON DELETE RESTRICT,
    entry_type entry_type NOT NULL DEFAULT 'not_set_yet',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Pedigree relationships
--     - Maps relationships between entries, e.g.:
--         - 'entry_1' member_of 'pop_A'
--         - 'entry_1.1' clone_of 'entry_1'
--         - 'entry_3' parent_is 'entry_1' AND 'entry_3' parent_is 'entry_2'
CREATE TABLE IF NOT EXISTS entry_relationships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    child_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    parent_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    rel_type relationship_type NOT NULL DEFAULT 'not_set_yet',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT no_self_reference
        CHECK (child_id <> parent_id),
    CONSTRAINT unique_relationship
        UNIQUE (child_id, parent_id, rel_type)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Experiments, e.g.:
--      - crop field trials
--      - animal clinical trials
--      - plant growth chamber experiments
--      - animal stall experiments
CREATE TABLE IF NOT EXISTS experiments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    start_date DATE NOT NULL DEFAULT now(),
    end_date DATE,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Sites (e.g., Hamilton, Tatura, Mildura)
CREATE TABLE IF NOT EXISTS sites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Treatments (e.g., none, control, eNpower, drought)
CREATE TABLE IF NOT EXISTS treatments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Layout
CREATE TABLE IF NOT EXISTS layouts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL, -- In Julia use: "replication=$replication|block=$block|row=$row|col=$col"
    replication INT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    block INT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    row INT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    col INT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT unique_layout
        UNIQUE (replication, block, row, col)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Measurement, e.g.:
--     - harvest_20260701
--     - visit_20260701
--     - measurement_202609
-- From here we can classify (programatically in Julia) the measurements by cropping or growing year and seasons, e.g.:
--      - measure_date = 2026/07/01 is in the 2026-2027 cropping year and winter season
--      - measure_date = 2026/08/01 is in the 2026-2027 growing year and considered:
--          + winter season if a harvest or yield measurement, or
--          + early spring season if an intermediate (non-harvest) measurement,
--          + assuming the threshold is 15 days into the first month of the season.
CREATE TABLE IF NOT EXISTS measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    measure_date DATE NOT NULL DEFAULT now(),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Traits (e.g., grain yield, milk yield, plant height, animal fertility)
CREATE TABLE IF NOT EXISTS traits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenotype measurements (all phenotype data are numeric for simplicity, i.e. convert categorical traits into numerics and just put notes on the traits table)
CREATE TABLE IF NOT EXISTS phenotype_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id UUID NOT NULL REFERENCES experiments(id),
    site_id UUID NOT NULL REFERENCES sites(id),
    treatment_id UUID NOT NULL REFERENCES treatments(id),
    layout_id UUID NOT NULL REFERENCES layouts(id),
    measurement_id UUID NOT NULL REFERENCES measurements(id),
    entry_id UUID NOT NULL REFERENCES entries(id),
    trait_id UUID NOT NULL REFERENCES traits(id),
    value DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CHECK (
        value IS NULL OR value = value
    ),
    CONSTRAINT unique_phenotype_data
        UNIQUE (experiment_id, site_id, treatment_id, layout_id, measurement_id, entry_id, trait_id)
);

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Reference genomes
CREATE TABLE IF NOT EXISTS reference_genomes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- VCF files
CREATE TABLE IF NOT EXISTS genotype_vcfs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    reference_genome_id UUID NOT NULL REFERENCES reference_genomes(id),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Genomes files (Genomes struct save as JLD2)
CREATE TABLE IF NOT EXISTS genomes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    genotype_vcf_id UUID NOT NULL REFERENCES genotype_vcfs(id),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes files (Phenomes struct save as JLD2)
CREATE TABLE IF NOT EXISTS phenomes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Fit files (Fit struct save as JLD2, i.e. genomic prediction model structs)
CREATE TABLE IF NOT EXISTS fits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Genomes --> Entries relationships
CREATE TABLE IF NOT EXISTS genome_entries (
    genome_id UUID REFERENCES genomes(id) ON DELETE CASCADE,
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    PRIMARY KEY (genome_id, entry_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Entries relationships
CREATE TABLE IF NOT EXISTS phenome_entries (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, entry_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Traits relationships
CREATE TABLE IF NOT EXISTS phenome_traits (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    trait_id UUID REFERENCES traits(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, trait_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Experiments relationships
CREATE TABLE IF NOT EXISTS phenome_experiments (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    experiment_id UUID REFERENCES experiments(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, experiment_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Sites relationships
CREATE TABLE IF NOT EXISTS phenome_sites (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    site_id UUID REFERENCES sites(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, site_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Treatments relationships
CREATE TABLE IF NOT EXISTS phenome_treatments (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    treatment_id UUID REFERENCES treatments(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, treatment_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Measruements relationships
CREATE TABLE IF NOT EXISTS phenome_measurements (
    phenome_id UUID REFERENCES phenomes(id) ON DELETE CASCADE,
    measurement_id UUID REFERENCES measurements(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, measurement_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_entry_rel_child  ON entry_relationships(child_id);
CREATE INDEX IF NOT EXISTS idx_entry_rel_parent ON entry_relationships(parent_id);
CREATE INDEX IF NOT EXISTS idx_entry_rel_type ON entry_relationships(rel_type);
CREATE INDEX IF NOT EXISTS idx_pheno_entry_trait ON phenotype_data(entry_id, trait_id);
CREATE INDEX IF NOT EXISTS idx_pheno_trait ON phenotype_data(trait_id);
CREATE INDEX IF NOT EXISTS idx_species_name_trgm ON species USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entries_name_trgm ON entries USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_experiments_name_trgm ON experiments USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sites_name_trgm ON sites USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_treatments_name_trgm ON treatments USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_traits_name_trgm ON traits USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_measurements_name_trgm ON measurements USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_layouts_name_trgm ON layouts USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_reference_genomes_name_trgm ON reference_genomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genotype_vcfs_name_trgm ON genotype_vcfs USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genomes_name_trgm ON genomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_phenomes_name_trgm ON phenomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_fits_name_trgm ON fits USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_reference_genomes_file_path_trgm ON reference_genomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genotype_vcfs_file_path_trgm ON genotype_vcfs USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genomes_file_path_trgm ON genomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_phenomes_file_path_trgm ON phenomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_fits_file_path_trgm ON fits USING gin (file_path gin_trgm_ops);

-- -- Query to retrieve phenotype data for specific traits, experiments, and measurement dates, including entry and parent information
-- SELECT
--     experiment.name AS experiment,
--     site.name AS site,
--     treatment.name AS treatment,
--     layout.replication AS replication,
--     layout.block AS block,
--     layout.row AS row,
--     layout.col AS col,
--     measurement.name AS measurement,
--     entry.name AS entry,
--     parent.name AS parent_entry,
--     trait.name AS trait,
--     phenotype_data.value AS value
-- FROM phenotype_data
-- JOIN experiments ON phenotype_data.experiment_id = experiments.id
-- JOIN sites ON phenotype_data.site_id = sites.id
-- JOIN treatments ON phenotype_data.treatment_id = treatments.id
-- JOIN layouts ON phenotype_data.layout_id = layouts.id
-- JOIN measurements ON phenotype_data.measurement_id = measurements.id
-- JOIN traits ON phenotype_data.trait_id = traits.id
-- JOIN entries ON phenotype_data.entry_id = entries.id
-- LEFT JOIN entry_relationships ON entries.id = entry_relationships.child_id
-- LEFT JOIN entries AS parent ON entry_relationships.parent_id = parent.id
-- WHERE traits.name IN ('yield', 'height')
--   AND entries.name IN ('entry_1', 'entry_2')
--   AND experiments.name IN ('trial_2025', 'trial_2026')
--   AND measurements.measure_date IN ('2025-01-01', '2025-12-31');
