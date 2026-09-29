WITH src AS (
    SELECT *
    FROM payments
    WHERE paid_at < '2024-12-23'
),
paid_quarter_summary AS (
    SELECT QUARTER(paid_at) AS paid_quarter, ROUND(AVG(amount), 2) AS avg_amount
    FROM src
    GROUP BY QUARTER(paid_at)
)
SELECT *
FROM paid_quarter_summary
ORDER BY avg_amount DESC, paid_quarter
LIMIT 3
