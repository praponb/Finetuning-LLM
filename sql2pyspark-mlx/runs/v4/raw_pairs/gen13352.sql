SELECT status, COUNT(DISTINCT amount) unique_orders, SUM(amount) total_amount FROM payments WHERE method <> 'cash' GROUP BY status HAVING SUM(amount) < 6500 ORDER BY status DESC LIMIT 10;
