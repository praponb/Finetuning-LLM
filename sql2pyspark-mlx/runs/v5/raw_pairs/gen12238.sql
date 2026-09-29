WITH valid_payments AS (
    SELECT *, COALESCE(method, 'unassigned') AS method_filled
    FROM payments
    WHERE status = 'SUCCESS'
),
PaidMonthSummary AS (
    SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, status, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
    FROM valid_payments
    GROUP BY DATE_TRUNC('MONTH', paid_at), status
)
SELECT paid_month, failed_count
FROM PaidMonthSummary
WHERE failed_count >= 3
ORDER BY paid_month
