WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_num FROM stores) SELECT id, city, opened_date, store_name FROM ranked_stores WHERE row_num = 1
