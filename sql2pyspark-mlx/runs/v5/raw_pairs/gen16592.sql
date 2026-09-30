WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) rnk FROM stores) SELECT id, city, sqft, store_name FROM top_stores WHERE rnk <= 3 ORDER BY store_name;
