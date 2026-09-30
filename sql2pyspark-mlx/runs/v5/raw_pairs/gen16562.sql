WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(status) AS status_lower
    FROM shipments
    WHERE delivered_date >= '2024-02-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        COUNT(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost
    FROM cleaned
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status_lower,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS cost_bucket,
        MAX(running_sum_cost) OVER (PARTITION BY order_id) AS order_id_max_running_sum_cost,
        LAG(cost, 1) OVER w_hist AS prev_cost
    FROM with_windows
    WHERE shipped_date < '2025-03-01'
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY cost, id)
),
cleaned_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(cost, cost_bucket) AS least_cost_cost_bucket,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM metrics
    WHERE carrier IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    CASE WHEN least_cost_cost_bucket >= 82 THEN 'HIGH' ELSE 'LOW' END AS least_cost_cost_bucket_tier,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    SUM(CASE WHEN carrier = 'UPS' THEN order_id_max_running_sum_cost ELSE 0 END) AS ups_order_id_max_running_sum_cost,
    GROUPING_ID() AS grouping_level
FROM cleaned_shipments
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', shipped_date), CASE WHEN least_cost_cost_bucket >= 82 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('WEEK', shipped_date)));
