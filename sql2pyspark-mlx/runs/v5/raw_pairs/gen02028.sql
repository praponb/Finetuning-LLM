SELECT * FROM (SELECT status, account_type, balance FROM accounts) src PIVOT (MAX(balance) AS value_sum, COUNT(1) AS item_count FOR account_type IN ('LOAN', 'CHECKING'))
