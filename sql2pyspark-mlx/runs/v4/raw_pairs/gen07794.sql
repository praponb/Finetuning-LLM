WITH recent_payments AS (
    SELECT *, HOUR(paid_at) AS paid_at_hour
    FROM payments
    WHERE method IN ('card', 'bank_transfer', 'cash') OR status IN ('SUCCESS', 'REFUNDED', 'FAILED')
),
method_summary AS (
    SELECT method, MIN(paid_at) AS first_paid_at
    FROM recent_payments
    GROUP BY method
)
SELECT method, first_paid_at
FROM method_summary
ORDER BY method DESC;
