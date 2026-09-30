WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        status,
        DATE_TRUNC('MONTH', paid_at) AS paid_at_period
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-04 08:00:00'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT method, id
    FROM cleaned
    INTERSECT ALL
    SELECT method, id
    FROM payments
    WHERE order_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.method = k.method AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(method, 1, 3) AS method_prefix,
        DATEDIFF(CURRENT_DATE(), paid_at_period) AS days_since_paid_at_period,
        COALESCE(method, 'N/A') AS method_filled
    FROM verified
    WHERE method IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, method
    FROM base
    EXCEPT
    SELECT status, method
    FROM payments
    WHERE amount BETWEEN 640 AND 1460
),
verified_payments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.status = k.status AND s.method = k.method
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(days_since_paid_at_period) OVER (PARTITION BY status) AS status_stddev_samp_days_since_paid_at_period,
        COUNT(days_since_paid_at_period) OVER (PARTITION BY method ORDER BY CAST(paid_at_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS days_since_paid_at_period_count_90d
    FROM verified_payments
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        status,
        paid_at_period,
        method_prefix,
        days_since_paid_at_period,
        method_filled,
        status_stddev_samp_days_since_paid_at_period,
        days_since_paid_at_period_count_90d,
        MAX(days_since_paid_at_period_count_90d) OVER (PARTITION BY method, status) AS method_max_days_since_paid_at_period_count_90d,
        COUNT(days_since_paid_at_period) OVER (PARTITION BY status ORDER BY CAST(paid_at_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS days_since_paid_at_period_count_30d,
        ROUND(days_since_paid_at_period_count_90d / SUM(days_since_paid_at_period_count_90d) OVER (PARTITION BY status), 4) AS days_since_paid_at_period_count_90d_share
    FROM metrics
)
SELECT
    id,
    status,
    paid_at_period,
    method_prefix,
    status_stddev_samp_days_since_paid_at_period,
    method_max_days_since_paid_at_period_count_90d,
    days_since_paid_at_period_count_90d_share
FROM with_windows
ORDER BY paid_at_period DESC, status;
