SELECT t.id, t.account_id, a.status
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.merchant IS NOT NULL AND a.balance <= 46000;
