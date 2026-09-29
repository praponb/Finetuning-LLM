SELECT
    id,
    paid_at,
    amount,
    ROUND(AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7
FROM payments
ORDER BY id DESC
