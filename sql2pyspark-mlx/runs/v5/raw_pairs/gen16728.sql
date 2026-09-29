select id, age, signup_date, is_active, substring(email, 1, 3) as email_prefix from customers where credit_limit is null
