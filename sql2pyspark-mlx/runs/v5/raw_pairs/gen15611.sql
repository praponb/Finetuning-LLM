SELECT * FROM (SELECT segment, country, credit_limit FROM customers) PIVOT (SUM(credit_limit) FOR country IN ('DE', 'JP', 'UK', 'US')) ORDER BY segment;
