SELECT cost AS value FROM shipments EXCEPT SELECT price AS value FROM products ORDER BY value DESC;
