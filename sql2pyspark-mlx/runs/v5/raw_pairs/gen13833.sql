SELECT * FROM (SELECT city, country, rating FROM suppliers WHERE country <> 'SG') PIVOT (COUNT(rating) FOR country IN ('SG', 'DE', 'TH')) ORDER BY city;
