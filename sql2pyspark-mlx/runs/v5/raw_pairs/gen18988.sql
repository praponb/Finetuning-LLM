SELECT id, sqft, DENSE_RANK() OVER (ORDER BY sqft) AS dense_rnk FROM stores WHERE sqft <> 5336;
