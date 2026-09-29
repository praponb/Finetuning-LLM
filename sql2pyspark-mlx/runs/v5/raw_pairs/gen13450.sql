SELECT id, balance
FROM accounts
WHERE balance < (SELECT MIN(balance) FROM accounts)
ORDER BY id DESC
LIMIT 5;
