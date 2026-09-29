WITH FilteredOrders AS (
    SELECT *
    FROM orders
    WHERE customer_id = 26
),
summary AS (
    SELECT
        YEAR(order_date) AS order_year,
        QUARTER(order_date) AS order_quarter,
        SUM(shipping_fee) AS total_shipping_fee,
        SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store_count
    FROM FilteredOrders
    GROUP BY YEAR(order_date), QUARTER(order_date)
)
SELECT *
FROM summary
ORDER BY total_shipping_fee DESC, order_quarter DESC
