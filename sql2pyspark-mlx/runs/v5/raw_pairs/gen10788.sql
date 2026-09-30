SELECT CASE WHEN p.category IN ('Sports', 'Home') THEN 'priority' ELSE 'other' END AS category_group, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
JOIN products p ON oi.product_id = p.id
GROUP BY CASE WHEN p.category IN ('Sports', 'Home') THEN 'priority' ELSE 'other' END;
