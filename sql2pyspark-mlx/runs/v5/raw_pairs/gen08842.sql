SELECT * FROM (SELECT segment, country, age FROM customers) PIVOT (SUM(age) FOR country IN ('JP', 'UK', 'US'))
