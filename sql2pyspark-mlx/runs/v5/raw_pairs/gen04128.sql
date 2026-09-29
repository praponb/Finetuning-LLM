SELECT merchant
FROM transactions
WHERE amount <= 1000
UNION
SELECT merchant
FROM transactions
WHERE merchant <> 'Grab'
ORDER BY merchant
