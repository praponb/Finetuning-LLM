SELECT status, 'account' AS source FROM accounts UNION SELECT status, 'subscription' AS source FROM subscriptions WHERE YEAR(end_date) >= 2023
