SELECT * FROM (SELECT city, country, credit_limit FROM customers) PIVOT (MAX(credit_limit) FOR country IN ('TH', 'SG', 'US', 'DE'));
