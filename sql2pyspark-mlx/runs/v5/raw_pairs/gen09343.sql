SELECT
    is_active,
    email,
    signup_date,
    credit_limit * age AS credit_limit_times_age
FROM customers
WHERE UPPER(email) LIKE '%OB.%' OR segment IN ('SMB', 'Consumer');
