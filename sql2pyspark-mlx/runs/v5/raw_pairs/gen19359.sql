SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (AVG(rating) FOR country IN ('DE', 'TH', 'SG', 'JP'));
