SELECT signup_date, is_active, name, REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain FROM customers WHERE credit_limit >= 4500 AND credit_limit BETWEEN 4800 AND 15500 ORDER BY 1 DESC, name DESC;
