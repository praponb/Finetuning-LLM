SELECT opened_date FROM stores WHERE UPPER(store_name) LIKE '%IAM%' UNION SELECT start_date AS opened_date FROM subscriptions;
