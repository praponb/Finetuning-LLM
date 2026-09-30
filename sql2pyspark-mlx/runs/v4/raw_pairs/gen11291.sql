WITH cleaned AS (
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        UPPER(status) AS status_upper,
        DATE_TRUNC('YEAR', opened_date) AS opened_date_period,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date > DATE '2023-11-24'
      AND status = 'CLOSED'
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) = 1
),
with_prev AS (
    SELECT *, LAG(opened_date_period) OVER (PARTITION BY account_type ORDER BY opened_date_period, id) AS prev_opened_date_period
    FROM ranked
),
flagged AS (
    SELECT *, CASE WHEN prev_opened_date_period IS NULL OR opened_date_period > prev_opened_date_period + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_accounts AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    APPROX_PERCENTILE(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    (UNIX_TIMESTAMP(MAX(opened_date_period)) - UNIX_TIMESTAMP(MIN(opened_date_period))) / 60 AS duration_min,
    COUNT(*) AS cnt,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag
FROM sessions_accounts
GROUP BY ROLLUP (CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', opened_date))
