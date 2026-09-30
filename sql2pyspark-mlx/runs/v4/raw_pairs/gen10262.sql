WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        rating,
        COALESCE(city, 'Unknown') AS city_filled,
        COALESCE(city, 'UNKNOWN') AS city_filled_2
    FROM suppliers
    WHERE city IS NOT NULL
      AND city <> 'Singapore'
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS FIRST, id) AS dedup_rank
    FROM cleaned
) x
WHERE dedup_rank = 1;
