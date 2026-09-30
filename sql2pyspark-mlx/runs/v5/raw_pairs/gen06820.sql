SELECT *, ROUND(sqft * 1.1, 2) AS sqft_scaled, CONCAT(region, ', ', city) AS region_city
FROM stores
