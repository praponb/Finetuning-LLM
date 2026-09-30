SELECT status, 'account' source FROM accounts UNION SELECT status, 'subscription' source FROM subscriptions WHERE end_date <= '2023-12-20' ORDER BY status DESC;
