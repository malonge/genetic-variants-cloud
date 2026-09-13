{{ config(materialized='view') }}

WITH genotypes AS (
    SELECT * FROM {{ ref('int_variant_sample_genotypes') }}
),

samples AS (
    SELECT * FROM {{ ref('stg_samples') }}
),

genotypes_with_ancestry AS (
    SELECT
        g.variant_id,
        g.chrom,
        g.pos,
        g.variant_type,
        g.mutation_class,
        g.sample_id,
        s.superpopulation,
        s.population,
        g.genotype_class,
        g.alt_allele_count
    FROM genotypes g
    INNER JOIN samples s ON g.sample_id = s.sample_id
),

pop_stats AS (
    SELECT
        variant_id,
        chrom,
        pos,
        variant_type,
        mutation_class,
        superpopulation,
        COUNT(DISTINCT sample_id) AS total_samples,
        COUNT(CASE WHEN genotype_class != 'MISSING' THEN 1 END) AS called_samples,
        COUNT(CASE WHEN genotype_class = 'HET' THEN 1 END) AS het_count,
        COUNT(CASE WHEN genotype_class = 'HOM_ALT' THEN 1 END) AS hom_alt_count,
        SUM(COALESCE(alt_allele_count, 0)) AS allele_count,
        COUNT(CASE WHEN genotype_class != 'MISSING' THEN 1 END) * 2 AS allele_number,
        ROUND(
            SUM(COALESCE(alt_allele_count, 0)) * 1.0 / 
            NULLIF(COUNT(CASE WHEN genotype_class != 'MISSING' THEN 1 END) * 2, 0),
            6
        ) AS population_af,
        ROUND(
            COUNT(CASE WHEN genotype_class = 'HET' THEN 1 END) * 1.0 / 
            NULLIF(COUNT(CASE WHEN genotype_class != 'MISSING' THEN 1 END), 0),
            6
        ) AS population_het_rate
    FROM genotypes_with_ancestry
    GROUP BY
        variant_id,
        chrom,
        pos,
        variant_type,
        mutation_class,
        superpopulation
)

SELECT * FROM pop_stats
