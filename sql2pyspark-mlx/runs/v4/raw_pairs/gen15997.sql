SELECT id, city, country FROM suppliers WHERE city IN ('New York', 'Singapore', 'Tokyo') UNION ALL SELECT id, city, country FROM suppliers WHERE rating > 3.2;
