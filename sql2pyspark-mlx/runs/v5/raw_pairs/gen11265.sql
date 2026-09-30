SELECT id, account_type, opened_date, PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS opened_date_pct_rank FROM accounts ORDER BY account_type;
