WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
)
-- Running sum of the flag = session number
SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps
