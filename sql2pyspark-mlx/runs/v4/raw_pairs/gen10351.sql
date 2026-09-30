SELECT city FROM suppliers WHERE city IN ('Berlin', 'London', 'New York') EXCEPT SELECT city FROM stores ORDER BY city;
