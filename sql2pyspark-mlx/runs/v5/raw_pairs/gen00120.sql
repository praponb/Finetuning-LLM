SELECT country, segment FROM customers INTERSECT SELECT country, segment FROM customers WHERE city IN ('Bangkok', 'New York', 'Tokyo')
