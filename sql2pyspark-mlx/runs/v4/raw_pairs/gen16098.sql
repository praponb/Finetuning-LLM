SELECT
    i.product_id,
    i.quantity,
    ss.store_id,
    ROUND(ss.units * 0.85, 2) AS units_scaled
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY units_scaled DESC, quantity DESC;
