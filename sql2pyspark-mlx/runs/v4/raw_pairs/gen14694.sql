SELECT * FROM (SELECT segment, city, credit_limit FROM customers) PIVOT (MAX(credit_limit) FOR city IN ('London', 'Tokyo', 'Chiang Mai', 'New York')) ORDER BY segment;
