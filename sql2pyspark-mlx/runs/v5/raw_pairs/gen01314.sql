WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price - quantity AS line_total,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 70 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NOT NULL
),
ranked AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC NULLS LAST, id) = 1
),
metrics AS (
    SELECT
        *,
        AVG(line_total) OVER w_part AS last7_avg_line_total,
        MAX(line_total) OVER w_part AS last7_max_line_total,
        MIN(line_total) OVER w_part AS last7_min_line_total,
        SUM(line_total) OVER w_part AS last7_sum_line_total
    FROM ranked
    WINDOW w_part AS (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        last7_avg_line_total,
        last7_max_line_total,
        last7_sum_line_total,
        line_total * last7_avg_line_total AS line_total_times_last7_avg_line_total
    FROM metrics
    WHERE last7_max_line_total BETWEEN 30 AND 61
)
SELECT
    id,
    unit_price,
    last7_avg_line_total,
    line_total_times_last7_avg_line_total
FROM base
ORDER BY line_total_times_last7_avg_line_total DESC, unit_price;
