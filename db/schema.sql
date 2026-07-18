-- PostgreSQL Schema for GenomicBreedingDB.jl

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Notes
-- All references to id columns are UUIDs and named with the "_id" suffix (e.g. species_id, entries_id, trait_id), which are generated using the uuidv7() function from the pgcrypto extension. 

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
-- Function to automatically update the updated_at column whenever a row is updated
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- Species
CREATE TABLE IF NOT EXISTS species (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    ploidy INT NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT check_ploidy
        CHECK (ploidy >= 0)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Entries table
--      - Entry names are unique across all species, i.e. no two entries can have the same name even if they are different species)
--      - Includes cultivars, populations, individuals, and families
CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    species_id UUID REFERENCES species(id) ON DELETE RESTRICT,
    entry_type entry_type NOT NULL DEFAULT 'not_set_yet',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Pedigree relationships
--     - Maps relationships between entries, e.g.:
--         - 'entry_1' member_of 'pop_A'
--         - 'entry_1.1' clone_of 'entry_1'
--         - 'entry_3' parent_is 'entry_1' AND 'entry_3' parent_is 'entry_2'
CREATE TABLE IF NOT EXISTS entry_relationships (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    child_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    parent_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    rel_type relationship_type NOT NULL DEFAULT 'not_set_yet',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    start_date DATE NOT NULL DEFAULT now(),
    end_date DATE,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT check_end_date
        CHECK (end_date IS NULL OR end_date >= start_date)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Sites (e.g., Hamilton, Tatura, Mildura)
CREATE TABLE IF NOT EXISTS sites (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Treatments (e.g., none, control, eNpower, drought)
CREATE TABLE IF NOT EXISTS treatments (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Layout (generic and more robust definition of plot identifiers over using plot identifiers as strings or numbers)
-- In Julia, the layout is set as the concatenation of replication, block, row, and col, e.g. "1-1-1-1" for replication 1, block 1, row 1, col 1,
--      which should prevent duplicate plot identifiers across different experiments, sites, and treatments, and also allow for more flexible and robust layout definitions.
CREATE TABLE IF NOT EXISTS layouts (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    replication INT NOT NULL,
    block INT NOT NULL,
    row INT NOT NULL,
    col INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Measurement, e.g.:
--     - harvest_20260701
--     - visit_20260701
--     - measurement_202609
-- From here we can classify (programatically in Julia) the measurements by cropping or growing year and seasons, e.g.:
--      - measure_date = 2026-07-01 is in the 2026-2027 cropping year and winter season
--      - measure_date = 2026-08-01 is in the 2026-2027 growing year and considered:
--          + winter season if a harvest or yield measurement, or
--          + early spring season if an intermediate (non-harvest) measurement,
--          + assuming the threshold is 15 days into the first month of the season.
CREATE TABLE IF NOT EXISTS measurements (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    measure_date DATE NOT NULL DEFAULT now(),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Traits (e.g., grain yield: biomass_T_per_ha, milk yield: milk_L_per_day, plant height: height_cm, animal fertility: litter_size or calving_interval_days)
-- Note: I have decided not to include a unit field here because there are just so many possible units, and 
--      I believe it is better to just put the units in the name of the traits just so that the units are in-your-face explicit, and
--      we can simply add more info in the notes field for each trait if needed.
CREATE TABLE IF NOT EXISTS traits (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Environmental variables (e.g., temperature, rainfall, humidity, soil moisture, soil pH, soil nutrients)
CREATE TABLE IF NOT EXISTS environment_variables (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenotype measurements (all phenotype data are numeric for simplicity, i.e. convert categorical traits into numerics and just put notes on the traits table)
-- Here, we can partition the phenotype_data table by experiment_id in the future as phenotype data becomes very large and partitioning by experiment_id will allow for more efficient queries and data management.
--      I think 10 partitions is a good starting point, but we can always add more partitions later if needed.
CREATE TABLE IF NOT EXISTS phenotype_data (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuidv7(),
    experiment_id UUID NOT NULL REFERENCES experiments(id),
    site_id UUID NOT NULL REFERENCES sites(id),
    treatment_id UUID NOT NULL REFERENCES treatments(id),
    layout_id UUID NOT NULL REFERENCES layouts(id),
    measurement_id UUID NOT NULL REFERENCES measurements(id),
    entry_id UUID NOT NULL REFERENCES entries(id),
    trait_id UUID NOT NULL REFERENCES traits(id),
    value DOUBLE PRECISION,
    -- PRIMARY KEY (id, experiment_id), -- for partitioning
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        value IS NULL OR (
            value != 'Infinity'::float8 AND
            value != '-Infinity'::float8
        )
    ),
    CONSTRAINT unique_phenotype_data
        UNIQUE (experiment_id, site_id, treatment_id, layout_id, measurement_id, entry_id, trait_id)
);
-- PARTITION BY HASH (experiment_id);
-- CREATE TABLE phenotype_data_p0 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 0);
-- CREATE TABLE phenotype_data_p1 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 1);
-- CREATE TABLE phenotype_data_p2 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 2);
-- CREATE TABLE phenotype_data_p3 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 3);
-- CREATE TABLE phenotype_data_p4 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 4);
-- CREATE TABLE phenotype_data_p5 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 5);
-- CREATE TABLE phenotype_data_p6 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 6);
-- CREATE TABLE phenotype_data_p7 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 7);
-- CREATE TABLE phenotype_data_p8 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 8);
-- CREATE TABLE phenotype_data_p9 PARTITION OF phenotype_data FOR VALUES WITH (MODULUS 10, REMAINDER 9);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Environmental measurements (all environment data are numeric for simplicity, i.e. convert categorical environment variables into numerics and just put notes on the environment variables table)
-- layout_id is included here to allow for spatially resolved environment data, e.g.:
--      soil moisture at different plots within a field trial, 
--      where for entire site-level environment data, the layout_id can be set to a default layout with a single plot (e.g., "1-1-1-1")
-- Similar to phenotype_data, we can do partitioning in the future, i.e. by experiment_id because as environment data becomes large and partitioning by experiment_id will allow for more efficient queries and data management.
CREATE TABLE IF NOT EXISTS environment_data (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuidv7(),
    experiment_id UUID NOT NULL REFERENCES experiments(id),
    site_id UUID NOT NULL REFERENCES sites(id),
    treatment_id UUID NOT NULL REFERENCES treatments(id),
    measurement_id UUID NOT NULL REFERENCES measurements(id),
    layout_id UUID NOT NULL REFERENCES layouts(id),
    environment_variable_id UUID NOT NULL REFERENCES environment_variables(id),
    value DOUBLE PRECISION,
    -- PRIMARY KEY (id, experiment_id), -- for partitioning
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        value IS NULL OR (
            value != 'Infinity'::float8 AND
            value != '-Infinity'::float8
        )
    ),
    CONSTRAINT unique_environment_data
        UNIQUE (experiment_id, site_id, treatment_id, layout_id, measurement_id, environment_variable_id)
);
-- PARTITION BY HASH (experiment_id);
-- CREATE TABLE environment_data_p0 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 0);
-- CREATE TABLE environment_data_p1 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 1);
-- CREATE TABLE environment_data_p2 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 2);
-- CREATE TABLE environment_data_p3 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 3);
-- CREATE TABLE environment_data_p4 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 4);
-- CREATE TABLE environment_data_p5 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 5);
-- CREATE TABLE environment_data_p6 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 6);
-- CREATE TABLE environment_data_p7 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 7);
-- CREATE TABLE environment_data_p8 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 8);
-- CREATE TABLE environment_data_p9 PARTITION OF environment_data FOR VALUES WITH (MODULUS 10, REMAINDER 9);

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Reference genomes
CREATE TABLE IF NOT EXISTS reference_genomes (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- VCF files
CREATE TABLE IF NOT EXISTS genotype_vcfs (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    reference_genome_id UUID NOT NULL REFERENCES reference_genomes(id),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Genomes files (Genomes struct save as JLD2)
CREATE TABLE IF NOT EXISTS genomes (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    genotype_vcf_id UUID NOT NULL REFERENCES genotype_vcfs(id),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes files (Phenomes struct save as JLD2)
CREATE TABLE IF NOT EXISTS phenomes (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Fit files (Fit struct save as JLD2, i.e. genomic prediction model structs)
CREATE TABLE IF NOT EXISTS fits (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    name TEXT UNIQUE NOT NULL,
    file_path TEXT UNIQUE NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Genomes --> Entries relationships
CREATE TABLE IF NOT EXISTS genome_entries (
    genome_id UUID NOT NULL REFERENCES genomes(id) ON DELETE CASCADE,
    entry_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    PRIMARY KEY (genome_id, entry_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Entries relationships
CREATE TABLE IF NOT EXISTS phenome_entries (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    entry_id UUID NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, entry_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Traits relationships
CREATE TABLE IF NOT EXISTS phenome_traits (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    trait_id UUID NOT NULL REFERENCES traits(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, trait_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Experiments relationships
CREATE TABLE IF NOT EXISTS phenome_experiments (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    experiment_id UUID NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, experiment_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Sites relationships
CREATE TABLE IF NOT EXISTS phenome_sites (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    site_id UUID NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, site_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Treatments relationships
CREATE TABLE IF NOT EXISTS phenome_treatments (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    treatment_id UUID NOT NULL REFERENCES treatments(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, treatment_id)
);

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Phenomes --> Measruements relationships
CREATE TABLE IF NOT EXISTS phenome_measurements (
    phenome_id UUID NOT NULL REFERENCES phenomes(id) ON DELETE CASCADE,
    measurement_id UUID NOT NULL REFERENCES measurements(id) ON DELETE CASCADE,
    PRIMARY KEY (phenome_id, measurement_id)
);

-------------------------------------------------------------------------------------------------------------------------------------------------------
-- Triggers to automatically update the updated_at column whenever a row is updated
CREATE TRIGGER trg_species_updated_at BEFORE UPDATE ON species FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_entries_updated_at BEFORE UPDATE ON entries FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_entry_relationships_updated_at BEFORE UPDATE ON entry_relationships FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_experiments_updated_at BEFORE UPDATE ON experiments FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_sites_updated_at BEFORE UPDATE ON sites FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_treatments_updated_at BEFORE UPDATE ON treatments FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_layouts_updated_at BEFORE UPDATE ON layouts FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_measurements_updated_at BEFORE UPDATE ON measurements FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_traits_updated_at BEFORE UPDATE ON traits FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_environment_variables_updated_at BEFORE UPDATE ON environment_variables FOR EACH ROW EXECUTE FUNCTION set_updated_at();
-- CREATE TRIGGER trg_phenotype_data_updated_at BEFORE UPDATE ON phenotype_data FOR EACH ROW EXECUTE FUNCTION set_updated_at(); -- commented-out for efficiency and will do manually in Julia when updating phenotype_data
-- CREATE TRIGGER trg_environment_data_updated_at BEFORE UPDATE ON environment_data FOR EACH ROW EXECUTE FUNCTION set_updated_at(); -- commented-out for efficiency and will do manually in Julia when updating phenotype_data
CREATE TRIGGER trg_reference_genomes_updated_at BEFORE UPDATE ON reference_genomes FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_genotype_vcfs_updated_at BEFORE UPDATE ON genotype_vcfs FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_genomes_updated_at BEFORE UPDATE ON genomes FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_phenomes_updated_at BEFORE UPDATE ON phenomes FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_fits_updated_at BEFORE UPDATE ON fits FOR EACH ROW EXECUTE FUNCTION set_updated_at();

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Indexes for performance
-- Phenomic and entries data main tables
CREATE INDEX IF NOT EXISTS idx_species_name_trgm ON species USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entries_name_trgm ON entries USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_experiments_name_trgm ON experiments USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sites_name_trgm ON sites USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_treatments_name_trgm ON treatments USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_layouts_name_trgm ON layouts USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_measurements_name_trgm ON measurements USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_traits_name_trgm ON traits USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_environment_variables_name_trgm ON environment_variables USING gin (name gin_trgm_ops);
-- Relationships and foreign keys
CREATE INDEX IF NOT EXISTS idx_entry_rel_child  ON entry_relationships(child_id);
CREATE INDEX IF NOT EXISTS idx_entry_rel_parent ON entry_relationships(parent_id);
CREATE INDEX IF NOT EXISTS idx_entry_rel_type ON entry_relationships(rel_type);
-- Species foreign key in entries table
CREATE INDEX IF NOT EXISTS idx_entries_species ON entries(species_id);
-- Phenotype data foreign keys
CREATE INDEX IF NOT EXISTS idx_pheno_experiment ON phenotype_data(experiment_id);
CREATE INDEX IF NOT EXISTS idx_pheno_site ON phenotype_data(site_id);
CREATE INDEX IF NOT EXISTS idx_pheno_treatment ON phenotype_data(treatment_id);
CREATE INDEX IF NOT EXISTS idx_pheno_layout ON phenotype_data(layout_id);
CREATE INDEX IF NOT EXISTS idx_pheno_measurement ON phenotype_data(measurement_id);
CREATE INDEX IF NOT EXISTS idx_pheno_trait ON phenotype_data(trait_id);
CREATE INDEX IF NOT EXISTS idx_pheno_entry ON phenotype_data(entry_id);
CREATE INDEX IF NOT EXISTS idx_pheno_entry_trait ON phenotype_data(entry_id, trait_id);
CREATE INDEX IF NOT EXISTS idx_pheno_trait_measurement ON phenotype_data(trait_id, measurement_id);
-- Environmental data foreign keys
CREATE INDEX IF NOT EXISTS idx_env_experiment ON environment_data(experiment_id);
CREATE INDEX IF NOT EXISTS idx_env_site ON environment_data(site_id);
CREATE INDEX IF NOT EXISTS idx_env_treatment ON environment_data(treatment_id);
CREATE INDEX IF NOT EXISTS idx_env_layout ON environment_data(layout_id);
CREATE INDEX IF NOT EXISTS idx_env_measurement ON environment_data(measurement_id);
CREATE INDEX IF NOT EXISTS idx_env_variable ON environment_data(environment_variable_id);
CREATE INDEX IF NOT EXISTS idx_env_variable_measurement ON environment_data(environment_variable_id, measurement_id);
-- Genomic data main tables
CREATE INDEX IF NOT EXISTS idx_reference_genomes_name_trgm ON reference_genomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genotype_vcfs_name_trgm ON genotype_vcfs USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genomes_name_trgm ON genomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_phenomes_name_trgm ON phenomes USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_fits_name_trgm ON fits USING gin (name gin_trgm_ops);
-- File path indexes for genomic data main tables
CREATE INDEX IF NOT EXISTS idx_reference_genomes_file_path_trgm ON reference_genomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genotype_vcfs_file_path_trgm ON genotype_vcfs USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_genomes_file_path_trgm ON genomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_phenomes_file_path_trgm ON phenomes USING gin (file_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_fits_file_path_trgm ON fits USING gin (file_path gin_trgm_ops);
-- Genomic data foreign keys
CREATE INDEX IF NOT EXISTS idx_genotype_vcfs_reference_genome ON genotype_vcfs(reference_genome_id);
CREATE INDEX IF NOT EXISTS idx_genomes_vcf ON genomes(genotype_vcf_id);
-- Genomes and phenomes relationships with entries, traits, experiments, sites, treatments, and measurements
CREATE INDEX IF NOT EXISTS idx_genome_entries_entry ON genome_entries(entry_id);
CREATE INDEX IF NOT EXISTS idx_phenome_entries_entry ON phenome_entries(entry_id);
CREATE INDEX IF NOT EXISTS idx_phenome_traits_trait ON phenome_traits(trait_id);
CREATE INDEX IF NOT EXISTS idx_phenome_experiments_experiment ON phenome_experiments(experiment_id);
CREATE INDEX IF NOT EXISTS idx_phenome_sites_site ON phenome_sites(site_id);
CREATE INDEX IF NOT EXISTS idx_phenome_treatments_treatment ON phenome_treatments(treatment_id);
CREATE INDEX IF NOT EXISTS idx_phenome_measurements_measurement ON phenome_measurements(measurement_id);
-- Additional indexes for phenotype_data and environment_data tables to improve query performance
CREATE INDEX idx_pheno_trait_site ON phenotype_data(trait_id, site_id);
CREATE INDEX idx_pheno_trait_experiment ON phenotype_data(trait_id, experiment_id);
CREATE INDEX idx_env_var_measurement ON environment_data(environment_variable_id, measurement_id);