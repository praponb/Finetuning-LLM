SELECT ss.product_id, s.store_name, ROUND(s.sqft, 1) sqft_rounded
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.store_id = 4
ORDER BY sqft_rounded;
