WITH top_store_sales AS (SELECT id, store_id, units, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS pos FROM store_sales) SELECT * FROM top_store_sales WHERE pos <= 3;
