SELECT id, txn_type
FROM transactions
WHERE amount > 1900
EXCEPT
SELECT id, txn_type
FROM transactions
WHERE merchant NOT IN ('7-Eleven', 'Grab', 'Starbucks');
