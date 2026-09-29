SELECT sale_date FROM store_sales WHERE sale_date >= '2025-03-05' INTERSECT SELECT end_date AS sale_date FROM subscriptions ORDER BY sale_date;
