SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS rn FROM orders) AS r WHERE rn <= 3;
