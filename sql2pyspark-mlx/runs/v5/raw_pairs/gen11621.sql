WITH ValidPayments AS (
    SELECT *
    FROM payments
    WHERE paid_at < '2024-04-01'
),
summary AS (
    SELECT method, COUNT(*) AS num_payments, MAX(paid_at) AS last_paid_at
    FROM ValidPayments
    GROUP BY method
)
SELECT *
FROM summary
WHERE num_payments > 9
