SELECT end_date FROM subscriptions WHERE status = 'EXPIRED' EXCEPT SELECT sale_date FROM store_sales ORDER BY end_date DESC;
