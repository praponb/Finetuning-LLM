WITH RecentOrders AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE order_date <= '2024-10-01' AND store_id IN (20, 21)
),
OrdersStats AS (
    SELECT
        status,
        channel,
        ROUND(AVG(amount), 2) AS avg_amount,
        SUM(CASE WHEN status = 'SHIPPED' THEN amount ELSE 0 END) AS shipped_amount
    FROM RecentOrders
    GROUP BY status, channel
)
SELECT
    status,
    channel,
    avg_amount,
    shipped_amount
FROM OrdersStats
WHERE shipped_amount > 48
ORDER BY avg_amount
