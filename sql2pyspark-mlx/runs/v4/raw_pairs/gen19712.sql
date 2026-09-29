WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price * quantity AS line_total
    FROM order_items
    WHERE order_id IN (1, 4, 16)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total NULLS FIRST, id) AS latest_rank
        FROM base
    ) t
    WHERE latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(line_total) OVER w_grp AS order_id_avg_line_total,
        MAX(line_total) OVER w_recent AS product_id_max_line_total,
        SUM(unit_price) OVER w_ordered AS cumulative_unit_price
    FROM latest
    WINDOW w_grp AS (PARTITION BY order_id), w_recent AS (PARTITION BY product_id), w_ordered AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN order_id_avg_line_total >= 81 THEN 'LARGE' ELSE 'SMALL' END AS order_id_avg_line_total_tier,
        CASE WHEN cumulative_unit_price >= 1882 THEN 'HIGH' ELSE 'LOW' END AS cumulative_unit_price_tier,
        APPROX_PERCENTILE(line_total, 0.9) AS line_total_p90,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(cumulative_unit_price, ARRAY(0.25, 0.5, 0.75)) AS cumulative_unit_price_quartiles,
        STDDEV_SAMP(quantity) AS quantity_stddev,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY CUBE (CASE WHEN order_id_avg_line_total >= 81 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_unit_price >= 1882 THEN 'HIGH' ELSE 'LOW' END)
    HAVING COUNT(*) < 1
)
SELECT
    order_id_avg_line_total_tier,
    cumulative_unit_price_tier,
    event_count,
    quantity_stddev,
    grouping_level
FROM grouped
ORDER BY quantity_stddev NULLS LAST
