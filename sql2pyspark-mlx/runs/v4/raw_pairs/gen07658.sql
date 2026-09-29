SELECT * FROM (SELECT country, segment, credit_limit FROM customers) PIVOT (AVG(credit_limit) FOR segment IN ('Corporate', 'Consumer'))
