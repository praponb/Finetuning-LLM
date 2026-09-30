WITH src AS (
    SELECT *, MONTH(paid_at) AS paid_at_month
    FROM payments
    WHERE method IN ('promptpay', 'bank_transfer') AND status IN ('SUCCESS', 'FAILED', 'REFUNDED')
),
payments_stats AS (
    SELECT status, COUNT(*) AS row_count, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
    FROM src
    GROUP BY status
)
SELECT *
FROM payments_stats
ORDER BY pct_success DESC, status
LIMIT 5;
