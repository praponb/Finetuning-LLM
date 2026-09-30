SELECT amount, order_date, store_id, DATE_ADD(order_date, 34) AS order_date_plus_34d FROM orders
