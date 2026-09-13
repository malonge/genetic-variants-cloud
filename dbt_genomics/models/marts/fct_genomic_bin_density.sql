{{ config(materialized='table') }}

WITH variants AS (
    SELECT
        chrom,
        pos,
        variant_id,
        variant_type,
        mutation_class,
        qual,
        primary_af,
        -- Genomic coordinate binning: 100kb intervals (100,000 bp)
        FLOOR(pos / 100000) * 100000 AS bin_start,
        FLOOR(pos / 100000) * 100000 + 100000 AS bin_end
    FROM {{ ref('stg_variants') }}
),

binned_summary AS (
    SELECT
        chrom,
        bin_start,
        bin_end,
        COUNT(DISTINCT variant_id) AS total_variants,
        COUNT(CASE WHEN variant_type = 'SNV' THEN 1 END) AS snv_count,
        COUNT(CASE WHEN variant_type = 'INSERTION' THEN 1 END) AS insertion_count,
        COUNT(CASE WHEN variant_type = 'DELETION' THEN 1 END) AS deletion_count,
        COUNT(CASE WHEN variant_type IN ('INSERTION', 'DELETION', 'COMPLEX_INDEL') THEN 1 END) AS indel_count,
        -- Density per kilobase (window length = 100kb)
        ROUND(COUNT(DISTINCT variant_id) * 1.0 / 100.0, 3) AS variant_density_per_kb,
        -- Transition vs Transversion within bin
        COUNT(CASE WHEN mutation_class = 'TRANSITION' THEN 1 END) AS transition_count,
        COUNT(CASE WHEN mutation_class = 'TRANSVERSION' THEN 1 END) AS transversion_count,
        ROUND(
            COUNT(CASE WHEN mutation_class = 'TRANSITION' THEN 1 END) * 1.0 /
            NULLIF(COUNT(CASE WHEN mutation_class = 'TRANSVERSION' THEN 1 END), 0),
            3
        ) AS titv_ratio,
        ROUND(AVG(qual), 2) AS mean_qual,
        ROUND(AVG(primary_af), 5) AS mean_af
    FROM variants
    GROUP BY
        chrom,
        bin_start,
        bin_end
)

SELECT * FROM binned_summary
ORDER BY chrom, bin_start
