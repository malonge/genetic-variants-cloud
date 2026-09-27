WITH source AS (
    SELECT * FROM read_csv('../data/bronze/samples/samples.tsv', delim='\t', header=true)
),

standardized AS (
    SELECT
        family_id,
        sample_id,
        father_id,
        mother_id,
        CASE sex
            WHEN 1 THEN 'MALE'
            WHEN 2 THEN 'FEMALE'
            ELSE 'UNKNOWN'
        END AS sex_label,
        sex AS sex_code,
        population,
        superpopulation,
        -- Indicator for unrelated probands (neither father nor mother in cohort)
        (father_id = '0' AND mother_id = '0') AS is_unrelated
    FROM source
)

SELECT * FROM standardized
