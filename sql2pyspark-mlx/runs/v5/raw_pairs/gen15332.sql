SELECT opened_date, 'account' AS source FROM accounts WHERE YEAR(opened_date) = 2025 UNION ALL SELECT start_date AS opened_date, 'subscription' AS source FROM subscriptions;
