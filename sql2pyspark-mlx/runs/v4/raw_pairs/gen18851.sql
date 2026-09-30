SELECT * FROM (SELECT segment, country, age FROM customers) src PIVOT (MAX(age) FOR country IN ('DE', 'JP'))
