SELECT channel, AVG(shipping_fee) AS avg_shipping_fee FROM orders WHERE order_date BETWEEN '2024-02-01' AND '2024-07-28' GROUP BY channel
