SELECT id, sqft, RANK() OVER (ORDER BY sqft DESC) AS rnk
FROM stores
ORDER BY sqft ASC, id DESC
