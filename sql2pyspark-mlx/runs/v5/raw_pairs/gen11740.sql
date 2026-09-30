SELECT id, merchant, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 3) amount_share FROM transactions WHERE merchant <> '7-Eleven' ORDER BY amount
