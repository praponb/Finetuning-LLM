SELECT id, DATE_SUB(order_date, 7) AS week_before FROM orders;
