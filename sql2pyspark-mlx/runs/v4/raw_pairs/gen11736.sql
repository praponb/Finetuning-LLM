WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        CASE WHEN cost >= 97 THEN 'High' WHEN cost >= 14 THEN 'Medium' ELSE 'Low' END AS cost_level,
        ADD_MONTHS(shipped_date, 6) AS shipped_date_plus_months
    FROM shipments
    WHERE shipped_date > DATE '2023-04-21'
      AND order_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', shipped_date_plus_months) AS year_start,
    STDDEV_SAMP(cost) AS cost_stddev,
    APPROX_PERCENTILE(cost, 0.9) AS cost_p90,
    PERCENTILE_APPROX(cost, 0.5) AS cost_p50,
    STDDEV_SAMP(cost) AS cost_stddev_2,
    STDDEV_SAMP(cost) AS cost_stddev_3,
    COUNT(*) FILTER (WHERE carrier = 'Kerry') AS kerry_rows
FROM base
GROUP BY DATE_TRUNC('YEAR', shipped_date_plus_months)
ORDER BY cost_stddev_3
