SELECT id, amount, NTILE(4) OVER (ORDER BY amount, id) AS quartile
FROM transactions
ORDER BY quartile
