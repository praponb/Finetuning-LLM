SELECT id, status, amount, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn FROM orders WHERE channel IN ('web', 'store') ORDER BY rn;
