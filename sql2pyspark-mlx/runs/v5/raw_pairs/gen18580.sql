WITH src AS (
    SELECT *, ROUND(amount, 2) AS amount_rounded
    FROM payments
    WHERE TO_DATE(paid_at) < '2023-04-01'
),
agg AS (
    SELECT
        status,
        method,
        AVG(amount_rounded) AS avg_amount_rounded,
        MIN(paid_at) AS min_paid_at
    FROM src
    GROUP BY status, method
)
SELECT status, method, avg_amount_rounded
FROM agg
WHERE avg_amount_rounded < 58
