WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled
    FROM subscriptions
    WHERE customer_id IN (4, 28, 33)
      AND end_date > DATE '2023-04-21'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        is_cancelled
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_cancelled, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM deduped
    EXCEPT
    SELECT status
    FROM subscriptions
    WHERE start_date < '2024-04-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.status = k.status
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        monthly_fee,
        status,
        is_cancelled,
        PERCENT_RANK() OVER w_hist AS monthly_fee_percent_rank,
        DENSE_RANK() OVER w_recent AS is_cancelled_dense_rank
    FROM kept_rows
    WHERE is_cancelled > 56
    WINDOW w_hist AS (PARTITION BY status ORDER BY monthly_fee DESC NULLS FIRST), w_recent AS (PARTITION BY plan ORDER BY is_cancelled NULLS LAST)
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY status) AS status_avg_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee,
        ROUND(is_cancelled_dense_rank / SUM(is_cancelled_dense_rank) OVER (PARTITION BY status), 4) AS is_cancelled_dense_rank_share,
        CUME_DIST() OVER (PARTITION BY status, plan ORDER BY is_cancelled DESC NULLS LAST) AS is_cancelled_cume_dist
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    status,
    COUNT_IF(plan = 'basic') AS basic_events,
    SUM(plan_avg_monthly_fee) AS total_plan_avg_monthly_fee,
    AVG(status_avg_monthly_fee) AS avg_status_avg_monthly_fee
FROM with_windows_subscriptions
GROUP BY 1;
