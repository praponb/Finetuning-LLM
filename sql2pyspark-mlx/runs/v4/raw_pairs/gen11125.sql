SELECT *, COALESCE(sqft, 0) AS sqft_filled
FROM stores
WHERE city = 'Chiang Mai'
ORDER BY region
