WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label
    FROM products
    WHERE launch_date >= TO_DATE('2023-05-08')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= '2024-12-01'
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity = 3
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        brand,
        price,
        cost
    FROM not_flagged
) src
UNPIVOT (
    reading FOR metric_name IN (price AS PRICE_VAL, cost AS COST)
)
