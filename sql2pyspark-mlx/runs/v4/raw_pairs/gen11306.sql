WITH flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM payments
)
SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM flagged;
