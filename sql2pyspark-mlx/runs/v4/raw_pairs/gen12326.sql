WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        WEEKOFYEAR(last_restock) AS last_restock_weekofyear
    FROM inventory
    WHERE last_restock >= DATE '2024-01-13'
)
-- Multi-level aggregation
SELECT
    store_id,
    APPROX_PERCENTILE(last_restock_weekofyear, 0.9) AS last_restock_weekofyear_p90,
    COUNT(*) AS event_count,
    SUM(last_restock_weekofyear) AS total_last_restock_weekofyear
FROM cleaned
GROUP BY store_id;
