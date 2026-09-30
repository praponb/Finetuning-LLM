SELECT country, name, credit_limit, ROUND(credit_limit, 0) AS credit_limit_rounded FROM customers ORDER BY credit_limit DESC;
