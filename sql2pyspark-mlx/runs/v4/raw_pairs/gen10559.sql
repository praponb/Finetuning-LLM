SELECT * FROM (SELECT YEAR(signup_date) AS signup_year, city, age FROM customers)
PIVOT (AVG(age) FOR city IN ('Berlin', 'New York'))
