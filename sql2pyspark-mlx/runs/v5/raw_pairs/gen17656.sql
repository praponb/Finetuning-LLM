WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at > TO_DATE('2023-11-01')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    COUNT(*) AS event_count,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(*) DESC) AS event_count_dense_rank
FROM islands
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', paid_at)
HAVING event_count > 1;
