SELECT 'store' AS origin, opened_date FROM stores WHERE YEAR(opened_date) = 2025 UNION ALL SELECT 'subscription' AS origin, end_date FROM subscriptions ORDER BY origin DESC
