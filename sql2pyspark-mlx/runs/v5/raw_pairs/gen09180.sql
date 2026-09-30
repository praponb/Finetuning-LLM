WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM inventory
    EXCEPT DISTINCT
    SELECT id, product_id
    FROM inventory
    WHERE last_restock BETWEEN '2023-03-01' AND '2025-07-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_ADD(last_restock, 34) AS last_restock_plus_34d
    FROM reconciled
    WHERE last_restock >= DATE '2023-05-10'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('week', last_restock) AS last_restock_period,
        LAST_DAY(last_restock_plus_34d) AS last_restock_plus_34d_month_end
    FROM valid
    WHERE last_restock > TO_DATE('2023-08-11')
      AND quantity BETWEEN 86 AND 145
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock_period,
        last_restock_plus_34d_month_end,
        CUME_DIST() OVER w_running AS quantity_cume_dist,
        SUM(quantity) OVER w_hist AS quantity_sum_7d,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS quantity_sum_90d
    FROM cleaned
    WHERE quantity != 192
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity DESC), w_hist AS (PARTITION BY product_id ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last_restock_period,
        last_restock_plus_34d_month_end,
        quantity_sum_7d,
        quantity_sum_90d,
        quantity_cume_dist - quantity_sum_90d AS quantity_cume_dist_minus_quantity_sum_90d,
        GREATEST(quantity_cume_dist, quantity_sum_7d) AS greatest_quantity_cume_dist_quantity_sum_7d
    FROM with_windows
    WHERE MONTH(last_restock_plus_34d_month_end) IN (8, 9, 12)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity_cume_dist_minus_quantity_sum_90d) OVER w_ordered AS quantity_cume_dist_minus_quantity_sum_90d_avg_30d,
        COUNT(greatest_quantity_cume_dist_quantity_sum_7d) OVER w_ordered AS greatest_quantity_cume_dist_quantity_sum_7d_count_30d,
        AVG(greatest_quantity_cume_dist_quantity_sum_7d) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS greatest_quantity_cume_dist_quantity_sum_7d_avg_7d,
        SUM(quantity_cume_dist_minus_quantity_sum_90d) OVER w_ordered AS quantity_cume_dist_minus_quantity_sum_90d_sum_30d
    FROM filtered
    WINDOW w_ordered AS (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (ORDER BY greatest_quantity_cume_dist_quantity_sum_7d_avg_7d DESC) AS greatest_quantity_cume_dist_quantity_sum_7d_avg_7d_cume_dist
    FROM metrics
)
SELECT
    last_restock_plus_34d_month_end,
    quantity_sum_7d,
    quantity_sum_90d,
    quantity_cume_dist_minus_quantity_sum_90d,
    quantity_cume_dist_minus_quantity_sum_90d_avg_30d,
    greatest_quantity_cume_dist_quantity_sum_7d_count_30d,
    greatest_quantity_cume_dist_quantity_sum_7d_avg_7d,
    quantity_cume_dist_minus_quantity_sum_90d_sum_30d,
    greatest_quantity_cume_dist_quantity_sum_7d_avg_7d_cume_dist
FROM enriched
