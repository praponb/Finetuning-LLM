WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('WEEK', last_restock) last_restock_period
    FROM inventory
    WHERE last_restock > DATE '2024-02-28'
)
-- Ranking and gap features
SELECT
    *,
    MIN(quantity) OVER (PARTITION BY store_id) store_id_min_quantity,
    AVG(quantity) OVER (PARTITION BY product_id) product_id_avg_quantity
FROM base
