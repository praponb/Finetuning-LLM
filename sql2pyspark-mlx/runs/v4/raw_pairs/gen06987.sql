SELECT age_bucket, avg_credit_limit
FROM (
    SELECT CASE WHEN age >= 72 THEN 'high' WHEN age >= 35 THEN 'mid' ELSE 'low' END age_bucket, ROUND(AVG(credit_limit), 2) avg_credit_limit
    FROM customers
    GROUP BY CASE WHEN age >= 72 THEN 'high' WHEN age >= 35 THEN 'mid' ELSE 'low' END
) agg
WHERE avg_credit_limit > 67;
