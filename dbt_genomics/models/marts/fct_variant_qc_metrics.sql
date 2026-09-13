{{ config(materialized='table') }}

WITH variants AS (
    SELECT
        variant_id,
        chrom,
        pos,
        variant_type,
        mutation_class,
        qual,
        primary_af,
        CASE
            WHEN qual < 50 THEN '1_LOW (<50)'
            WHEN qual < 200 THEN '2_MEDIUM (50-200)'
            WHEN qual < 1000 THEN '3_HIGH (200-1000)'
            ELSE '4_VERY_HIGH (>=1000)'
        END AS qual_tier,
        CASE
            WHEN primary_af IS NULL THEN 'UNKNOWN'
            WHEN primary_af < 0.01 THEN 'RARE (<1%)'
            WHEN primary_af < 0.05 THEN 'LOW_FREQ (1-5%)'
            ELSE 'COMMON (>=5%)'
        END AS frequency_tier
    FROM {{ ref('stg_variants') }}
),

qc_calibration AS (
    SELECT
        qual_tier,
        frequency_tier,
        COUNT(DISTINCT variant_id) AS variant_count,
        COUNT(CASE WHEN variant_type = 'SNV' THEN 1 END) AS snv_count,
        COUNT(CASE WHEN variant_type != 'SNV' THEN 1 END) AS indel_count,
        COUNT(CASE WHEN mutation_class = 'TRANSITION' THEN 1 END) AS transition_count,
        COUNT(CASE WHEN mutation_class = 'TRANSVERSION' THEN 1 END) AS transversion_count,
        ROUND(
            COUNT(CASE WHEN mutation_class = 'TRANSITION' THEN 1 END) * 1.0 /
            NULLIF(COUNT(CASE WHEN mutation_class = 'TRANSVERSION' THEN 1 END), 0),
            3
        ) AS titv_ratio,
        ROUND(AVG(qual), 2) AS mean_qual
    FROM variants
    GROUP BY
        qual_tier,
        frequency_tier
)

SELECT * FROM qc_calibration
ORDER BY qual_tier, frequency_tier
