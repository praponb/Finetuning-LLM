SELECT id, brand
FROM products
WHERE brand IN ('Stark', 'Initech')
INTERSECT
SELECT id, brand
FROM products
WHERE category IN ('Fashion', 'Books', 'Home');
