SELECT *, ROUND(balance * 100, 2) AS balance_adjusted FROM accounts ORDER BY opened_date, account_type DESC
