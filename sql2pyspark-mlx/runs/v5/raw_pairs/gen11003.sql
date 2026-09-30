WITH matched_keys AS (
    SELECT store_id
    FROM inventory
    EXCEPT ALL
    SELECT store_id
    FROM inventory
    WHERE last_restock < '2025-07-01'
),
kept_rows AS (
    SELECT s.*
    FROM inventory s
    JOIN matched_keys k ON s.store_id = k.store_id
)
SELECT
    store_id,
    CASE WHEN quantity >= 248 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY ROLLUP (store_id, CASE WHEN quantity >= 248 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY quantity_p90 DESC;
