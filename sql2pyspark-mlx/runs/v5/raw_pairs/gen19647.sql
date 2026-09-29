SELECT COUNT(DISTINCT price) AS unique_brands
FROM products
WHERE category = 'Books' AND cost BETWEEN 360 AND 580;
