SELECT status, order_date FROM orders WHERE amount BETWEEN 840 AND 1090 UNION ALL SELECT status, order_date FROM orders WHERE status != 'RETURNED' ORDER BY order_date
