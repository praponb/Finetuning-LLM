SELECT id, status, amount, method FROM (SELECT id, status, amount, method, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn FROM payments) AS r WHERE rn = 1;
