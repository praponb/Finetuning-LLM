SELECT * FROM accounts QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) = 1;
