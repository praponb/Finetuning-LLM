WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity * 0.85, 2) AS quantity_scaled
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, p.cost
    FROM cleaned e
    LEFT JOIN products p
        ON e.product_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, quantity_scaled - cost AS quantity_scaled_minus_cost
    FROM enriched
    WHERE product_id IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    NTILE(3) OVER w_hist AS unit_price_bucket,
    SUM(quantity_scaled) OVER (PARTITION BY product_id ORDER BY quantity_scaled_minus_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_scaled,
    LAG(quantity_scaled, 1, 0) OVER (ORDER BY quantity_scaled_minus_cost, id) AS prev_quantity_scaled,
    ROW_NUMBER() OVER w_recent AS row_seq
FROM valid
WINDOW w_hist AS (PARTITION BY product_id ORDER BY unit_price DESC, id), w_recent AS (PARTITION BY product_id ORDER BY quantity_scaled_minus_cost, id)
