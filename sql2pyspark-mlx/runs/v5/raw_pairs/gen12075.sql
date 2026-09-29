WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) rnk FROM stores) SELECT id, region, sqft FROM ranked_stores WHERE rnk <= 2
