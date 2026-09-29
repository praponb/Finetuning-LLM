WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS is_active
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-05-11')
      AND status IN ('CANCELLED', 'PAUSED', 'EXPIRED')
),
valid_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(start_date, 3) AS start_date_plus_months
    FROM valid
    WHERE end_date > DATE '2023-10-15'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status, plan ORDER BY start_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY is_active DESC) AS is_active_percent_rank,
        SUM(is_active) OVER (PARTITION BY status) AS status_sum_is_active
    FROM valid_subscriptions
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY first_known_monthly_fee DESC NULLS FIRST, id) AS rn
        FROM scored
    ) t
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', start_date_plus_months) AS quarter_start,
    CASE WHEN status_sum_is_active >= 428 THEN 'LARGE' ELSE 'SMALL' END AS status_sum_is_active_tier,
    DATE_TRUNC('WEEK', start_date) AS week_start,
    ROUND(AVG(is_active_percent_rank), 2) AS avg_is_active_percent_rank,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'PAUSED' THEN 1 END), 0), 2) AS monthly_fee_per_paused,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    AVG(first_known_monthly_fee) FILTER (WHERE status = 'CANCELLED') AS cancelled_avg_first_known_monthly_fee
FROM ranked
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', start_date_plus_months), CASE WHEN status_sum_is_active >= 428 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', start_date)), (DATE_TRUNC('QUARTER', start_date_plus_months), CASE WHEN status_sum_is_active >= 428 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY status_sum_is_active_tier;
