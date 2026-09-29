WITH numbered AS (SELECT id, city, sqft, store_name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rnk FROM stores) SELECT id, city, sqft, store_name FROM numbered WHERE rnk <= 3
