SELECT id, REGEXP_EXTRACT(email, '@([a-z.]+)', 1) AS domain FROM customers;
