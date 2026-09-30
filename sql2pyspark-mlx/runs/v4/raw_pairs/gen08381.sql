SELECT
    id,
    status,
    method,
    paid_at,
    amount,
    order_id,
    ROUND(AVG(amount) OVER (PARTITION BY status, method ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num
FROM payments
WHERE method = 'paypal'
