SELECT * FROM (SELECT CASE WHEN amount >= 4200 THEN 'high' WHEN amount >= 2000 THEN 'mid' ELSE 'low' END AS amount_bucket, merchant, amount FROM transactions WHERE amount <= 1700)
PIVOT (MAX(amount) FOR merchant IN ('Shell', 'Amazon', '7-Eleven', 'Starbucks'))
ORDER BY amount_bucket
