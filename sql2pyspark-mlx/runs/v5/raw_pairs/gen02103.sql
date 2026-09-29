SELECT id, balance, ROW_NUMBER() OVER (ORDER BY balance, id) AS balance_row
FROM accounts;
