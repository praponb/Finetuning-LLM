SELECT CASE WHEN discount >= 0.39 THEN 'high' WHEN discount >= 0.13 THEN 'mid' ELSE 'low' END discount_bucket, MIN(order_date) first_order_date, MAX(amount) max_amount
FROM orders
WHERE discount IS NOT NULL
GROUP BY 1;
