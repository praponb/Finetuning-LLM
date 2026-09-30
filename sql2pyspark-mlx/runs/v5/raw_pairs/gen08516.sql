WITH valid_payments AS (
    SELECT *, ROUND(amount * 1.15, 2) AS amount_scaled
    FROM payments
    WHERE order_id IN (10, 11, 21, 37) OR amount BETWEEN 850 AND 1490
),
payments_stats AS (
    SELECT TO_DATE(paid_at) AS paid_day, SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) AS cash_count
    FROM valid_payments
    GROUP BY TO_DATE(paid_at)
)
SELECT *
FROM payments_stats
ORDER BY paid_day DESC, cash_count DESC;
