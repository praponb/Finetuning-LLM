SELECT product_id, cnt FROM (SELECT product_id, COUNT(*) AS cnt FROM inventory WHERE last_restock <= '2024-09-24' GROUP BY product_id) AS x WHERE cnt > 4;
