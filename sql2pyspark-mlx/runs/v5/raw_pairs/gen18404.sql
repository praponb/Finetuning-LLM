SELECT i.id, i.product_id
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE region NOT IN ('Central', 'South', 'North'))
