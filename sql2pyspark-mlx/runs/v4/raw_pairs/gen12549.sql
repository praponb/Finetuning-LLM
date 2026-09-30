WITH valid_orders AS (
    SELECT *, CASE WHEN discount >= 0.36 THEN 'High' WHEN discount >= 0.06 THEN 'Medium' ELSE 'Low' END AS discount_level
    FROM orders
    WHERE discount <= 0.25 OR discount BETWEEN 0.24 AND 0.33
),
order_quarter_summary AS (
    SELECT QUARTER(order_date) AS order_quarter, SUM(CASE WHEN status = 'COMPLETED' THEN shipping_fee ELSE 0 END) AS completed_shipping_fee, SUM(CASE WHEN status = 'COMPLETED' THEN discount ELSE 0 END) AS completed_discount
    FROM valid_orders
    GROUP BY QUARTER(order_date)
)
SELECT *
FROM order_quarter_summary
WHERE completed_shipping_fee > 56
ORDER BY completed_shipping_fee DESC;
