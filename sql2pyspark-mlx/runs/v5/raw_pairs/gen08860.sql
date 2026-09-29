SELECT *, LAST_DAY(order_date) order_date_month_end FROM orders WHERE (order_date BETWEEN '2024-11-01' AND '2025-01-01' AND shipping_fee <= 20) OR channel = 'partner'
