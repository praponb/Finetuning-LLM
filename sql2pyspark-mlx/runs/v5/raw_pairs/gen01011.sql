SELECT city, age FROM customers WHERE UPPER(name) LIKE '%AVI%' UNION SELECT city, age FROM customers WHERE age <= 59 ORDER BY city;
