SELECT id, txn_type, merchant
FROM transactions
WHERE account_id IN (3, 37)
UNION
SELECT id, txn_type, merchant
FROM transactions
WHERE merchant = 'Grab'
