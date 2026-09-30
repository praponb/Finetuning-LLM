WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity - unit_price AS line_total,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity = 7
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price_filled) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_unit_price_filled,
        LAG(unit_price_filled) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_unit_price_filled,
        LAG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS seq_no
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_unit_price,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_unit_price,
        SUM(unit_price_filled) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_filled
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        unit_price,
        prev_unit_price_filled,
        running_unit_price_filled,
        AVG(quantity) OVER w_hist AS quantity_avg_5_prior,
        SUM(prev_unit_price) OVER (ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_unit_price,
        LEAD(quantity, 1) OVER w_part AS next_quantity
    FROM with_windows
    WHERE last4_avg_unit_price > 170
    WINDOW w_hist AS (ORDER BY next_unit_price_filled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_part AS (ORDER BY line_total, id)
)
SELECT
    id,
    quantity,
    unit_price,
    prev_unit_price_filled,
    quantity_avg_5_prior,
    cumulative_prev_unit_price
FROM enriched
ORDER BY quantity_avg_5_prior NULLS FIRST;
