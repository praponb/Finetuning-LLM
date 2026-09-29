SELECT merchant, account_id
FROM transactions
WHERE amount > 2300
UNION ALL
SELECT merchant, account_id
FROM transactions
WHERE is_flagged;
