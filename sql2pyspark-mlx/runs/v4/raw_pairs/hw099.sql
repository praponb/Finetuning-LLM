SELECT * FROM (
    SELECT segment, country, credit_limit FROM customers
) PIVOT (
    SUM(credit_limit) FOR country IN ('TH', 'US', 'SG')
);
