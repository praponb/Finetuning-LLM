SELECT country, signup_date, city, age + credit_limit AS age_plus_credit_limit, CEIL(credit_limit) AS credit_limit_whole FROM customers WHERE is_active AND credit_limit > 14000 AND city IS NULL;
