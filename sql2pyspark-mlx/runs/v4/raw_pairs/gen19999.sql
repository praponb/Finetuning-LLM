SELECT CASE WHEN discount >= 0.43 THEN 'high' WHEN discount >= 0.17 THEN 'mid' ELSE 'low' END discount_bucket, MIN(amount) min_amount
FROM orders
WHERE channel = 'partner' OR channel IN ('mobile', 'partner', 'store')
GROUP BY discount_bucket;
