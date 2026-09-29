SELECT p.cost, t.total_unit_price, t.quantity_range
FROM products p
JOIN (SELECT product_id, SUM(unit_price) AS total_unit_price, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.quantity_range > 27;
