WITH Base AS (
    SELECT *
    FROM payments
    WHERE order_id IN (13, 18) OR amount BETWEEN 1260 AND 1580
),
status_summary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_cash, COUNT(amount) amount_count
    FROM Base
    GROUP BY status
)
SELECT *
FROM status_summary
WHERE pct_cash >= 67
ORDER BY amount_count DESC, pct_cash ASC;
