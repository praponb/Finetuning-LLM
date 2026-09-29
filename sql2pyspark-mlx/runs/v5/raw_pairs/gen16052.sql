SELECT *, INITCAP(TRIM(email)) AS email_title FROM customers WHERE NOT (credit_limit < 13800 AND LENGTH(email) > 10);
