SELECT id, sqft
FROM stores
WHERE sqft >= (SELECT MAX(sqft) FROM stores WHERE opened_date BETWEEN '2024-05-01' AND '2024-11-01')
ORDER BY id DESC;
