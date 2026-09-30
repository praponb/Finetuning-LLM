WITH recent_payments AS (
    SELECT *, CEIL(amount) AS amount_whole
    FROM payments
    WHERE amount > 950
),
Summary AS (
    SELECT status, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, MIN(paid_at) AS first_paid_at
    FROM recent_payments
    GROUP BY status, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT status, paid_cohort, first_paid_at
FROM Summary
ORDER BY first_paid_at DESC, paid_cohort, status
LIMIT 5
