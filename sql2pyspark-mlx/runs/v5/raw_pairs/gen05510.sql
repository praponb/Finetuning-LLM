WITH metrics AS (
    SELECT
        id,
        order_id,
        product_id,
        MIN(quantity) OVER w_running running_min_quantity,
        SUM(quantity) OVER w_running running_sum_quantity,
        AVG(quantity) OVER w_running running_avg_quantity,
        COUNT(quantity) OVER w_running running_count_quantity
    FROM order_items
    WINDOW w_running AS (PARTITION BY order_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT id, product_id
    FROM metrics
    EXCEPT ALL
    SELECT id, product_id
    FROM order_items
    WHERE order_id IN (8, 19, 28, 38)
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
metrics_order_items AS (
    SELECT
        *,
        STDDEV_SAMP(running_count_quantity) OVER (PARTITION BY product_id, order_id) product_id_stddev_samp_running_count_quantity,
        CUME_DIST() OVER (ORDER BY running_min_quantity DESC NULLS FIRST) running_min_quantity_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY running_min_quantity DESC) running_min_quantity_percent_rank
    FROM kept_rows
    WHERE product_id = 11
),
kept_keys AS (
    SELECT product_id, id
    FROM metrics_order_items
    INTERSECT
    SELECT product_id, id
    FROM order_items
    WHERE quantity >= 3
),
verified AS (
    SELECT s.*
    FROM metrics_order_items s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    order_id,
    kpi,
    reading
FROM (
    SELECT
        order_id,
        running_min_quantity_percent_rank,
        running_sum_quantity,
        running_min_quantity
    FROM verified
) src
UNPIVOT (
    reading FOR kpi IN (running_min_quantity_percent_rank, running_sum_quantity, running_min_quantity)
)
