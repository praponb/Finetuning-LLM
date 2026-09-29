WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        quantity * unit_price AS line_total,
        ROUND(quantity * 1.07, 2) AS quantity_scaled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (3, 28, 31)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        line_total,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        MIN(quantity) OVER (PARTITION BY product_id, order_id) AS product_id_min_quantity,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id) AS corr_unit_price_quantity
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cumulative_unit_price, 1) OVER (PARTITION BY order_id ORDER BY cumulative_unit_price, id) AS next_cumulative_unit_price,
        SUM(line_total) OVER (PARTITION BY order_id ORDER BY corr_unit_price_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_line_total,
        SUM(corr_unit_price_quantity) OVER (PARTITION BY order_id) AS order_id_sum_corr_unit_price_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY product_id_min_quantity, id) AS event_seq
    FROM metrics
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.line_total,
        e.cumulative_line_total,
        e.order_id_sum_corr_unit_price_quantity,
        e.event_seq,
        o.amount
    FROM with_windows e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
-- Window metrics over several different partitions
SELECT
    id,
    line_total,
    cumulative_line_total,
    event_seq,
    COUNT(cumulative_line_total) OVER w_recent AS last4_count_cumulative_line_total,
    AVG(cumulative_line_total) OVER w_recent AS last4_avg_cumulative_line_total,
    MAX(cumulative_line_total) OVER w_recent AS last4_max_cumulative_line_total,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY order_id_sum_corr_unit_price_quantity NULLS FIRST) AS order_id_sum_corr_unit_price_quantity_dense_rank
FROM with_dims
WINDOW w_recent AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
