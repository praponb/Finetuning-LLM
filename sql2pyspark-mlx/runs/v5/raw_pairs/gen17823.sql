SELECT * FROM (SELECT segment, city, credit_limit FROM customers WHERE credit_limit < 8600) PIVOT (SUM(credit_limit) FOR city IN ('New York', 'Bangkok', 'London', 'Tokyo')) ORDER BY segment;
