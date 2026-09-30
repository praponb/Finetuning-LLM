WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.sqft,
        b.city
    FROM inventory a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        d.product_name,
        d.launch_date,
        d.cost
    FROM with_dims src
    LEFT JOIN products d
        ON src.product_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock NULLS FIRST, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS sqft_per_tokyo,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM deduped
GROUP BY 1
HAVING chiang_mai_count >= 1
