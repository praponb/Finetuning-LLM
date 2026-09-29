WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(quantity, unit_price) least_quantity_unit_price,
        COALESCE(unit_price, 0) unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    order_id,
    product_id,
    quantity,
    least_quantity_unit_price,
    SUM(least_quantity_unit_price) OVER w_running running_sum_least_quantity_unit_price,
    MAX(least_quantity_unit_price) OVER w_running running_max_least_quantity_unit_price,
    MIN(least_quantity_unit_price) OVER w_running running_min_least_quantity_unit_price,
    COUNT(least_quantity_unit_price) OVER w_running running_count_least_quantity_unit_price
FROM filtered
WINDOW w_running AS (PARTITION BY product_id ORDER BY unit_price_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
