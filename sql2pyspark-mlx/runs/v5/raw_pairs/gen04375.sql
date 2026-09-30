SELECT id, account_type, opened_date, PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS opened_date_pct_rank FROM accounts WHERE opened_date >= '2024-02-26'
