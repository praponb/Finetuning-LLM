WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS pos FROM stores) SELECT id, city, opened_date, sqft FROM ranked_stores WHERE pos <= 3;
