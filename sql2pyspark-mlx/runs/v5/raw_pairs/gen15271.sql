SELECT end_date FROM subscriptions WHERE status = 'EXPIRED' UNION SELECT launch_date AS end_date FROM products WHERE stock_qty BETWEEN 129 AND 329 ORDER BY end_date;
