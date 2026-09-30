SELECT id, method, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS rn FROM payments) AS r WHERE rn <= 3
