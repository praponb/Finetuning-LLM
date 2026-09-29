SELECT * FROM (SELECT segment, city, credit_limit FROM customers WHERE country = 'US') PIVOT (AVG(credit_limit) FOR city IN ('Bangkok', 'Berlin'))
