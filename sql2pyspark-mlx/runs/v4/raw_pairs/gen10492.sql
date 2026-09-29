WITH recent_orders AS (
    SELECT *, LAST_DAY(order_date) AS order_date_month_end
    FROM orders
    WHERE shipping_fee IS NOT NULL AND amount BETWEEN 1180 AND 1370
),
status_summary AS (
    SELECT status, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
    FROM recent_orders
    GROUP BY status
)
SELECT status, pct_shipped
FROM status_summary
ORDER BY status;
