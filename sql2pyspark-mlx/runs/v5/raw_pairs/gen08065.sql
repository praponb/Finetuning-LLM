SELECT * FROM (SELECT city, segment, age FROM customers)
PIVOT (SUM(age) FOR segment IN ('SMB', 'Corporate'))
