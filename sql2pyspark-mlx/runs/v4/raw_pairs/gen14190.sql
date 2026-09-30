SELECT status, ROUND(balance, 0) balance_rounded
FROM accounts
WHERE MONTH(opened_date) IN (1, 2, 8) AND account_type IN ('SAVINGS', 'LOAN')
