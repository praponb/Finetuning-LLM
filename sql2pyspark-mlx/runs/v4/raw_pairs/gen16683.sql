SELECT city, 'supplier' source
FROM suppliers
UNION
SELECT city, 'store' source
FROM stores
WHERE sqft != 4739;
