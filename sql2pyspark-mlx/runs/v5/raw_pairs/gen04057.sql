SELECT * FROM (SELECT segment, country, age FROM customers)
PIVOT (SUM(age) FOR country IN ('SG', 'TH', 'UK', 'JP'))
ORDER BY segment;
