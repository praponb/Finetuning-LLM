WITH ranked_store_sales AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) row_num FROM store_sales) SELECT id, store_id, units FROM ranked_store_sales WHERE row_num <= 3
