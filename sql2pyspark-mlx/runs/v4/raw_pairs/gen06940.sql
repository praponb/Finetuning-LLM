SELECT *, ROUND(credit_limit, 0) AS credit_limit_rounded
FROM customers
WHERE LENGTH(name) > 8
ORDER BY country DESC, credit_limit DESC;
