WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        region,
        sqft,
        CONCAT_WS(',', region, city) region_city_key,
        LENGTH(region) region_length
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date BETWEEN '2025-03-01' AND '2025-04-30'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(sqft, 0) sqft_rounded,
        ROUND(region_length * 0.9, 2) region_length_scaled
    FROM base
    WHERE sqft = 7133
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        sqft,
        region_city_key,
        region_length,
        sqft_rounded,
        region_length_scaled
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped
    EXCEPT ALL
    SELECT id
    FROM stores
    WHERE opened_date >= '2023-03-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    sqft,
    region_length,
    sqft_rounded,
    region_length_scaled
FROM reconciled
ORDER BY id DESC NULLS LAST
