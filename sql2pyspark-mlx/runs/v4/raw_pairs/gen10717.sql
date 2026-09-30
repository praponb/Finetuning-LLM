WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.new_group_flag,
    s.burst_id,
    CASE WHEN method RLIKE '^(cash|bank_transfer)$' THEN 'CORE' WHEN method RLIKE '^(card|paypal)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS method_zone,
    (SELECT MAX(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS max_payment_amount
FROM sessions_payments s
