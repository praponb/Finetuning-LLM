WITH ranked_products AS (SELECT id, brand, cost, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS pos FROM products) SELECT * FROM ranked_products WHERE pos <= 3
