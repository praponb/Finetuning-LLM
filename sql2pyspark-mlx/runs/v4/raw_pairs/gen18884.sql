WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        DATE_TRUNC('MONTH', opened_date) opened_date_period
    FROM stores
    WHERE sqft >= 12593
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity,
        x.inventory_last_restock
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date_period
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity_2,
        x.last_restock
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity_2, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN inventory_quantity_2 >= 268 THEN 'HIGH' ELSE 'LOW' END AS inventory_quantity_2_tier,
    city,
    region,
    SUM(CASE WHEN city = 'Chiang Mai' THEN inventory_quantity ELSE 0 END) AS chiang_mai_inventory_quantity,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows,
    PERCENTILE_APPROX(inventory_quantity, ARRAY(0.25, 0.5, 0.75)) AS inventory_quantity_quartiles,
    GROUPING_ID() AS gid
FROM with_latest_stores
GROUP BY GROUPING SETS ((CASE WHEN inventory_quantity_2 >= 268 THEN 'HIGH' ELSE 'LOW' END, city, region), (CASE WHEN inventory_quantity_2 >= 268 THEN 'HIGH' ELSE 'LOW' END, city), (CASE WHEN inventory_quantity_2 >= 268 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY inventory_quantity_2_tier DESC NULLS FIRST;
