SELECT id, status, amount, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS rn FROM payments ORDER BY rn DESC
