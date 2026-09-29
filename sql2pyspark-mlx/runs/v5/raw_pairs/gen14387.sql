SELECT stores.city, ROUND(STDDEV(inventory.quantity), 2) stddev_quantity
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
JOIN products ON inventory.product_id = products.id
GROUP BY stores.city
ORDER BY 1 DESC, stddev_quantity;
