SELECT * FROM (SELECT city, country, rating FROM suppliers) PIVOT (SUM(rating) FOR country IN ('JP', 'DE', 'US', 'SG'))
