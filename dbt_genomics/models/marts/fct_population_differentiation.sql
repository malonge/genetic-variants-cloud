{{ config(materialized='table') }}

WITH pop_stats AS (
    SELECT * FROM {{ ref('int_pop_variant_stats') }}
),

pivoted AS (
    SELECT
        variant_id,
        chrom,
        pos,
        variant_type,
        mutation_class,
        MAX(CASE WHEN superpopulation = 'AFR' THEN population_af END) AS af_afr,
        MAX(CASE WHEN superpopulation = 'AMR' THEN population_af END) AS af_amr,
        MAX(CASE WHEN superpopulation = 'EAS' THEN population_af END) AS af_eas,
        MAX(CASE WHEN superpopulation = 'EUR' THEN population_af END) AS af_eur,
        MAX(CASE WHEN superpopulation = 'SAS' THEN population_af END) AS af_sas,
        MAX(CASE WHEN superpopulation = 'AFR' THEN population_het_rate END) AS het_rate_afr,
        MAX(CASE WHEN superpopulation = 'EUR' THEN population_het_rate END) AS het_rate_eur
    FROM pop_stats
    GROUP BY
        variant_id,
        chrom,
        pos,
        variant_type,
        mutation_class
),

differentiated AS (
    SELECT
        *,
        -- Allele frequency divergence (Max AF minus Min AF across continental ancestries)
        ROUND(
            GREATEST(COALESCE(af_afr, 0), COALESCE(af_amr, 0), COALESCE(af_eas, 0), COALESCE(af_eur, 0), COALESCE(af_sas, 0)) -
            LEAST(COALESCE(af_afr, 1), COALESCE(af_amr, 1), COALESCE(af_eas, 1), COALESCE(af_eur, 1), COALESCE(af_sas, 1)),
            5
        ) AS max_af_divergence,
        CASE
            WHEN (
                GREATEST(COALESCE(af_afr, 0), COALESCE(af_amr, 0), COALESCE(af_eas, 0), COALESCE(af_eur, 0), COALESCE(af_sas, 0)) -
                LEAST(COALESCE(af_afr, 1), COALESCE(af_amr, 1), COALESCE(af_eas, 1), COALESCE(af_eur, 1), COALESCE(af_sas, 1))
            ) >= 0.30 THEN TRUE
            ELSE FALSE
        END AS is_highly_differentiated
    FROM pivoted
)

SELECT * FROM differentiated
ORDER BY chrom, pos
