WITH recent_payments AS (
    SELECT *, LENGTH(method) AS method_length
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS') AND (amount >= 710 OR method IN ('promptpay', 'card', 'bank_transfer'))
),
payments_stats AS (
    SELECT
        status,
        method,
        AVG(amount) AS avg_amount,
        SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END) AS bank_transfer_count
    FROM recent_payments
    GROUP BY status, method
)
SELECT status, method, bank_transfer_count
FROM payments_stats
ORDER BY 1, method
LIMIT 3
