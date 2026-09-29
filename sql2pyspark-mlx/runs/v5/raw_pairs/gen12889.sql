SELECT id, channel, amount, store_id FROM (SELECT *, RANK() OVER (PARTITION BY channel ORDER BY amount) rn FROM orders) r WHERE rn <= 3;
