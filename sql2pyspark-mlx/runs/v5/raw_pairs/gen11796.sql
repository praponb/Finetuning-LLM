SELECT signup_date, LOWER(city) AS city_lower, age * credit_limit AS age_times_credit_limit
FROM customers
WHERE segment IN ('Consumer', 'Corporate', 'SMB') OR name IS NULL;
