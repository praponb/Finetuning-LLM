SELECT * FROM (SELECT city, country, age FROM customers) PIVOT (AVG(age) FOR country IN ('DE', 'UK', 'JP', 'US')) ORDER BY city
