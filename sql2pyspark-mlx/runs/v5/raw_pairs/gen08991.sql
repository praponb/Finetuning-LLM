WITH cleaned AS (
    SELECT
        *,
        CONCAT(store_name, ' - ', region) store_name_region,
        CASE WHEN sqft >= 16649 THEN 'High' WHEN sqft >= 6226 THEN 'Medium' ELSE 'Low' END sqft_level
    FROM stores
    WHERE opened_date > TO_DATE('2023-09-16')
),
filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        sqft_level,
        CASE WHEN sqft >= 15436 THEN 'High' WHEN sqft >= 5957 THEN 'Medium' ELSE 'Low' END sqft_tier
    FROM cleaned
    WHERE opened_date > DATE '2023-08-04'
      AND region IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.customer_id IS NULL
        )
),
base AS (
    SELECT *, LOWER(city) city_lower
    FROM kept
    WHERE region IS DISTINCT FROM 'East'
      AND store_name LIKE 'Ai%'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
stats AS (
    SELECT
        region,
        ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS sqft_per_london,
        ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS sqft_per_chiang_mai,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM deduped
    GROUP BY 1
    HAVING sqft_per_london >= 0
)
SELECT
    region,
    sqft_per_london,
    sqft_per_chiang_mai
FROM stats
ORDER BY sqft_per_chiang_mai NULLS FIRST;
