SELECT id, account_type, opened_date, RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS rnk FROM accounts ORDER BY id DESC
