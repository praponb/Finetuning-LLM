SELECT CASE WHEN amount >= 4200 THEN 'high' WHEN amount >= 1100 THEN 'mid' ELSE 'low' END amount_bucket, COUNT(amount) amount_count
FROM transactions
WHERE amount <= 1100 AND amount > 1800 AND merchant = 'Starbucks'
GROUP BY 1
ORDER BY amount_count DESC
