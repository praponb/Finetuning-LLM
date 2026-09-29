SELECT credit_limit, segment, TRIM(city) AS clean_city FROM customers WHERE email LIKE '%@company.co' AND (segment IS NULL OR email LIKE '%@outlook.com');
