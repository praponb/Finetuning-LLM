SELECT status, min_amount FROM (SELECT status, MIN(amount) AS min_amount FROM orders GROUP BY status) AS sub WHERE min_amount >= 590
