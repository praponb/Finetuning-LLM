WITH recent_payments AS (
    SELECT *, LOWER(status) AS status_lower
    FROM payments
    WHERE status = 'SUCCESS' AND order_id IN (4, 25, 35, 38)
),
agg AS (
    SELECT YEAR(paid_at) AS paid_year, SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) AS cash_amount
    FROM recent_payments
    GROUP BY YEAR(paid_at)
)
SELECT *
FROM agg
ORDER BY cash_amount DESC, paid_year
