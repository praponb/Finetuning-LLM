WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(status, ' ', carrier) status_carrier,
        DATEDIFF(CURRENT_DATE(), delivered_date) days_since_delivered_date
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'DHL'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        days_since_delivered_date,
        COUNT(days_since_delivered_date) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_days_since_delivered_date,
        AVG(days_since_delivered_date) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_days_since_delivered_date,
        MAX(days_since_delivered_date) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_days_since_delivered_date,
        MIN(days_since_delivered_date) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_days_since_delivered_date,
        MIN(days_since_delivered_date) OVER (PARTITION BY order_id) order_id_min_days_since_delivered_date
    FROM base
    WHERE delivered_date > '2024-01-23'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY last7_min_days_since_delivered_date, id) last7_min_days_since_delivered_date_bucket,
        SUM(last7_count_days_since_delivered_date) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last7_count_days_since_delivered_date
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last7_avg_days_since_delivered_date,
        last7_max_days_since_delivered_date,
        last7_min_days_since_delivered_date,
        last7_min_days_since_delivered_date_bucket,
        running_last7_count_days_since_delivered_date,
        LEAST(last7_min_days_since_delivered_date, last7_count_days_since_delivered_date) least_last7_min_days_since_delivered_date_last7_count_days_since_delivered_date
    FROM with_windows
    WHERE order_id_min_days_since_delivered_date = 43
)
-- Multi-level aggregation
SELECT
    CASE WHEN last7_max_days_since_delivered_date >= 61 THEN 'LARGE' ELSE 'SMALL' END AS last7_max_days_since_delivered_date_tier,
    CASE WHEN last7_min_days_since_delivered_date >= 82 THEN 'LARGE' ELSE 'SMALL' END AS last7_min_days_since_delivered_date_tier,
    ROUND(AVG(last7_min_days_since_delivered_date), 2) AS avg_last7_min_days_since_delivered_date,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(last7_min_days_since_delivered_date, 0.95) AS last7_min_days_since_delivered_date_p95,
    RANK() OVER (PARTITION BY CASE WHEN last7_max_days_since_delivered_date >= 61 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(last7_min_days_since_delivered_date), 2) DESC) AS avg_last7_min_days_since_delivered_date_rank
FROM valid
GROUP BY last7_max_days_since_delivered_date_tier, last7_min_days_since_delivered_date_tier
