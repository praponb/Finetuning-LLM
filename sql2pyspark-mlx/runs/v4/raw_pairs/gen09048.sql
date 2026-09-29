SELECT id, is_active, email, credit_limit, ROUND(age, 2) AS age_rounded FROM customers ORDER BY age_rounded DESC;
