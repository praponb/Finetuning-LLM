WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.quantity,
        latest.last_restock
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    region,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN city = 'London' THEN quantity ELSE 0 END) AS london_quantity,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM with_last
GROUP BY region
ORDER BY region;
