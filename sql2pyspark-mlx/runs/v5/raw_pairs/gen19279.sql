WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LENGTH(status) AS status_length
    FROM payments
    WHERE amount BETWEEN 810 AND 970
      AND method IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        paid_at,
        status,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM sessions_payments
GROUP BY 1, 2
ORDER BY activity_month NULLS FIRST;
