SELECT SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count
FROM transactions
WHERE account_id = 21;
