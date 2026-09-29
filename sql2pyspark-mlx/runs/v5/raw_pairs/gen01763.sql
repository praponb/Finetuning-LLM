SELECT
    id,
    city,
    segment,
    name,
    ROUND(credit_limit / age, 2) credit_limit_per_age,
    CASE WHEN credit_limit >= 14000 THEN 'High' WHEN credit_limit >= 7000 THEN 'Medium' ELSE 'Low' END credit_limit_tier
FROM customers
WHERE LOWER(TRIM(email)) = 'carol@company.co' AND signup_date <= '2024-05-01'
ORDER BY segment DESC, id
LIMIT 10;
