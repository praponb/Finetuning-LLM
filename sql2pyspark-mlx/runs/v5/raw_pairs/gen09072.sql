WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity * 1.1, 2) AS quantity_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity) OVER w_hist AS running_min_quantity,
        COUNT(quantity) OVER w_hist AS running_count_quantity,
        AVG(quantity) OVER w_hist AS running_avg_quantity,
        SUM(quantity) OVER w_hist AS running_sum_quantity,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY order_id ORDER BY quantity_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM filtered
    WHERE product_id IN (10, 16, 26)
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.shipping_fee,
        o.channel
    FROM with_windows src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity_adjusted AS INT) AS quantity_adjusted_int
    FROM enriched
    WHERE channel IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(running_avg_quantity * 1.07, 2) AS adjusted_running_avg_quantity,
        ROUND(running_count_quantity * 1.15, 2) AS running_count_quantity_adjusted,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM valid
    WHERE product_id IS NOT NULL
      AND unit_price >= 340
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity_adjusted,
    s.running_min_quantity,
    s.running_count_quantity,
    s.running_avg_quantity,
    s.running_sum_quantity,
    s.shipping_fee,
    (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS min_store_sale_units
FROM base s;
