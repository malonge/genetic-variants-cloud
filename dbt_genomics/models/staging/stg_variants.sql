WITH source AS (
    SELECT * FROM read_parquet('../data/bronze/variants/**/*.parquet', hive_partitioning=true)
),

cleaned AS (
    SELECT
        chrom,
        pos,
        COALESCE(id, chrom || ':' || CAST(pos AS VARCHAR) || ':' || ref || ':' || list_extract(alt, 1)) AS variant_id,
        ref,
        alt,
        list_extract(alt, 1) AS primary_alt,
        len(alt) AS alt_allele_count,
        CASE
            WHEN len(ref) = 1 AND len(list_extract(alt, 1)) = 1 THEN 'SNV'
            WHEN len(ref) > len(list_extract(alt, 1)) THEN 'DELETION'
            WHEN len(ref) < len(list_extract(alt, 1)) THEN 'INSERTION'
            ELSE 'COMPLEX_INDEL'
        END AS variant_type,
        qual,
        filter,
        list_contains(filter, 'PASS') OR len(filter) = 0 AS is_pass,
        info_dp,
        info_an,
        info_ac,
        info_af,
        CASE 
            WHEN len(info_af) > 0 THEN list_extract(info_af, 1)
            ELSE NULL
        END AS primary_af,
        -- Detect Transition vs Transversion for SNVs
        CASE
            WHEN len(ref) = 1 AND len(list_extract(alt, 1)) = 1 AND (
                (ref = 'A' AND list_extract(alt, 1) = 'G') OR
                (ref = 'G' AND list_extract(alt, 1) = 'A') OR
                (ref = 'C' AND list_extract(alt, 1) = 'T') OR
                (ref = 'T' AND list_extract(alt, 1) = 'C')
            ) THEN 'TRANSITION'
            WHEN len(ref) = 1 AND len(list_extract(alt, 1)) = 1 THEN 'TRANSVERSION'
            ELSE 'NON_SNV'
        END AS mutation_class,
        samples
    FROM source
)

SELECT * FROM cleaned
