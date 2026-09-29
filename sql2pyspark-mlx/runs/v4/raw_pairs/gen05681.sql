SELECT id, merchant
FROM transactions
WHERE amount > 1900
UNION ALL
SELECT id, merchant
FROM transactions
WHERE is_flagged = TRUE
