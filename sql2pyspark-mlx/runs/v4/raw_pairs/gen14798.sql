WITH valid AS (
    SELECT *, DATE_TRUNC('week', start_date) AS start_date_period
    FROM subscriptions
    WHERE start_date >= DATE '2023-09-30'
      AND customer_id IS NOT NULL
      AND end_date <= '2023-04-01'
),
base AS (
    SELECT
        *,
        CAST(monthly_fee AS INT) AS monthly_fee_int,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM valid
    WHERE customer_id IS NOT NULL
      AND TO_DATE(start_date_period) < '2025-03-01'
),
prev_events AS (
    SELECT *, LAG(start_date_period) OVER (PARTITION BY status ORDER BY start_date_period, id) AS prev_start_date_period
    FROM base
),
flagged AS (
    SELECT *, CASE WHEN prev_start_date_period IS NULL OR start_date_period > prev_start_date_period + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY start_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_dims AS (
    SELECT a.*, b.segment
    FROM islands a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(prev_start_date_period) OVER (PARTITION BY plan ORDER BY prev_start_date_period, id) IS NULL
                 OR prev_start_date_period > LAG(prev_start_date_period) OVER (PARTITION BY plan ORDER BY prev_start_date_period, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session_2
    FROM with_dims
),
islands_subscriptions AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY plan ORDER BY prev_start_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    id,
    customer_id,
    plan,
    monthly_fee,
    status,
    start_date_period,
    monthly_fee_int,
    end_date_dayofweek,
    prev_start_date_period,
    CASE WHEN plan RLIKE '^(basic|premium)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS plan_zone
FROM islands_subscriptions;
