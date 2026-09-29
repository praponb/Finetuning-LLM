SELECT *, CAST(amount AS INT) AS amount_int
FROM transactions
WHERE merchant NOT IN ('Lazada', 'Amazon', 'Starbucks')
ORDER BY amount_int DESC
