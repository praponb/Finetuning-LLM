SELECT end_date AS event_date FROM subscriptions WHERE monthly_fee IS NOT NULL INTERSECT SELECT sale_date AS event_date FROM store_sales;
