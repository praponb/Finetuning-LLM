SELECT o.customer_id, o.amount, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE o.shipping_fee BETWEEN 35 AND 38 AND YEAR(c.signup_date) >= 2024
