WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE delivered_date > DATE '2023-08-30'
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee,
        b.status AS order_status,
        b.order_date
    FROM base s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipping_fee NULLS FIRST, id) AS row_num
        FROM with_dims
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(cost, 2) OVER (ORDER BY order_date, id) AS next_cost,
        AVG(days_since_shipped_date) OVER (PARTITION BY status) AS status_avg_days_since_shipped_date
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cost >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    PERCENTILE_APPROX(status_avg_days_since_shipped_date, 0.9) AS status_avg_days_since_shipped_date_p90,
    ROUND(SUM(status_avg_days_since_shipped_date) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS status_avg_days_since_shipped_date_per_returned
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN cost >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', delivered_date)), (CASE WHEN cost >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING status_avg_days_since_shipped_date_p90 > 0
ORDER BY status_avg_days_since_shipped_date_per_returned NULLS FIRST;
