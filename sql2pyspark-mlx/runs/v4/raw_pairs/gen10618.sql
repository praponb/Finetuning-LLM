SELECT * FROM (SELECT country, segment, age FROM customers) PIVOT (MAX(age) FOR segment IN ('Corporate', 'Consumer', 'SMB')) ORDER BY country;
