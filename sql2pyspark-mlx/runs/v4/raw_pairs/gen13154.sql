WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
)
SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM with_gaps;
