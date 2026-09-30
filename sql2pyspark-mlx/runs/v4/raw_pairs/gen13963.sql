WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(method, 'Unknown') AS method_filled,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE method IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        AVG(is_new_session) OVER (ORDER BY is_new_session, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_new_session_avg_6_prior,
        AVG(is_new_session) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_new_session_avg_30d
    FROM sessions_payments
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY amount DESC, id)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    CASE WHEN burst_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    COUNT_IF(method = 'promptpay') AS promptpay_events,
    MIN(paid_at) AS first_seen
FROM scored
GROUP BY DATE_TRUNC('MONTH', paid_at), CASE WHEN burst_id >= 9 THEN 'HIGH' ELSE 'LOW' END
HAVING refunded_count > 1
ORDER BY refunded_count DESC NULLS LAST
