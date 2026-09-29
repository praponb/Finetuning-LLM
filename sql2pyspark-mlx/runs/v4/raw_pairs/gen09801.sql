SELECT id, channel, amount, store_id FROM (SELECT id, channel, amount, store_id, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) row_num FROM orders) t WHERE row_num <= 3;
