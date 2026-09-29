WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 9 AND 12
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(amount, 2) OVER (PARTITION BY order_id, method ORDER BY paid_at, id) AS prev_amount,
        LEAD(new_group_flag, 1) OVER (PARTITION BY status, method ORDER BY burst_id, id) AS next_new_group_flag,
        NTILE(4) OVER (PARTITION BY method ORDER BY burst_id, id) AS burst_id_bucket,
        LEAD(new_group_flag, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_new_group_flag_2
    FROM sessions_payments
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_amount >= 1380 THEN 'LARGE' ELSE 'SMALL' END AS prev_amount_tier,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(next_new_group_flag_2, ARRAY(0.25, 0.5, 0.75)) AS next_new_group_flag_2_quartiles,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY ROLLUP (CASE WHEN prev_amount >= 1380 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', paid_at))
ORDER BY prev_amount_tier
