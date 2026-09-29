WITH cleaned AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', delivered_date) delivered_date_period,
        UPPER(carrier) carrier_upper
    FROM shipments
    WHERE shipped_date >= DATE '2023-08-03'
      AND YEAR(delivered_date) >= 2025
),
metrics AS (
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        carrier_upper,
        MAX(cost) OVER (PARTITION BY status) status_max_cost,
        NTILE(5) OVER (ORDER BY cost DESC, id) cost_bucket
    FROM cleaned
    WHERE delivered_date > '2024-10-01'
),
with_windows AS (
    SELECT
        id,
        shipped_date,
        cost,
        MIN(status_max_cost) OVER (PARTITION BY carrier ORDER BY status_max_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_status_max_cost,
        COUNT(status_max_cost) OVER (PARTITION BY carrier ORDER BY status_max_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_status_max_cost,
        MAX(status_max_cost) OVER (PARTITION BY carrier ORDER BY status_max_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_status_max_cost,
        SUM(status_max_cost) OVER (PARTITION BY carrier ORDER BY status_max_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_status_max_cost
    FROM metrics
),
filtered AS (
    SELECT
        id,
        last3_count_status_max_cost,
        last3_max_status_max_cost,
        last3_sum_status_max_cost,
        LEAST(cost, last3_count_status_max_cost) least_cost_last3_count_status_max_cost
    FROM with_windows
    WHERE last3_min_status_max_cost IS NOT NULL
)
SELECT
    CASE WHEN least_cost_last3_count_status_max_cost >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_cost_last3_count_status_max_cost_tier,
    CASE WHEN last3_max_status_max_cost >= 83 THEN 'HIGH' ELSE 'LOW' END AS last3_max_status_max_cost_tier,
    APPROX_PERCENTILE(least_cost_last3_count_status_max_cost, 0.5) AS least_cost_last3_count_status_max_cost_p50,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(CASE WHEN least_cost_last3_count_status_max_cost >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM filtered
GROUP BY ROLLUP (CASE WHEN least_cost_last3_count_status_max_cost >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last3_max_status_max_cost >= 83 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY last3_max_status_max_cost_tier;
