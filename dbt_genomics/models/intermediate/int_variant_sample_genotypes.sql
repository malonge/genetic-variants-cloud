{{ config(materialized='view') }}

WITH variants AS (
    SELECT
        variant_id,
        chrom,
        pos,
        ref,
        primary_alt,
        variant_type,
        qual,
        mutation_class,
        samples
    FROM {{ ref('stg_variants') }}
),

unnested_samples AS (
    SELECT
        v.variant_id,
        v.chrom,
        v.pos,
        v.ref,
        v.primary_alt,
        v.variant_type,
        v.mutation_class,
        s.sample_id,
        s.gt,
        s.gq,
        s.dp,
        -- Standardize genotype classification
        CASE
            WHEN s.gt IN ('0/0', '0|0') THEN 'HOM_REF'
            WHEN s.gt IN ('0/1', '0|1', '1/0', '1|0') THEN 'HET'
            WHEN s.gt IN ('1/1', '1|1') THEN 'HOM_ALT'
            WHEN s.gt IN ('./.', '.|.', '.') THEN 'MISSING'
            ELSE 'OTHER'
        END AS genotype_class,
        -- Count alternate allele dosage (0, 1, or 2)
        CASE
            WHEN s.gt IN ('0/0', '0|0') THEN 0
            WHEN s.gt IN ('0/1', '0|1', '1/0', '1|0') THEN 1
            WHEN s.gt IN ('1/1', '1|1') THEN 2
            ELSE NULL
        END AS alt_allele_count
    FROM variants v,
    UNNEST(v.samples) AS t(s)
)

SELECT * FROM unnested_samples
