SELECT id, customer_id, ROUND(discount / amount, 2) AS discount_per_amount FROM orders;
