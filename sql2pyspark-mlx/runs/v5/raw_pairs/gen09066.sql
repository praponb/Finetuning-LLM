WITH ranked_store_sales AS (SELECT id, store_id, units, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS pos FROM store_sales) SELECT * FROM ranked_store_sales WHERE pos <= 3;
