WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('MONTH', opened_date) AS opened_date_period
    FROM stores
    WHERE city = 'London'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(opened_date_period) OVER (PARTITION BY region ORDER BY opened_date_period, id) AS prev_opened_date_period
    FROM filtered
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_opened_date_period IS NULL OR opened_date_period > prev_opened_date_period + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY region ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_period,
        prev_opened_date_period,
        new_group_flag,
        burst_id,
        PERCENT_RANK() OVER w_grp AS new_group_flag_percent_rank,
        SUM(sqft) OVER w_grp_2 AS cumulative_sqft
    FROM sessionized
    WINDOW w_grp AS (PARTITION BY region ORDER BY new_group_flag DESC NULLS FIRST), w_grp_2 AS (ORDER BY prev_opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', prev_opened_date_period) AS activity_month,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    MIN(prev_opened_date_period) AS first_prev_opened_date_period,
    MIN(opened_date_period) AS started_at
FROM metrics
GROUP BY 1
ORDER BY east_count DESC NULLS LAST
