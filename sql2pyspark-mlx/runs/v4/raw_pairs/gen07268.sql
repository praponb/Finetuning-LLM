SELECT opened_date FROM accounts UNION SELECT end_date opened_date FROM subscriptions WHERE start_date <= '2025-09-01' ORDER BY opened_date;
