SELECT MONTH(products.launch_date) AS launch_month, COUNT(*) AS order_item_count
FROM order_items
JOIN products ON order_items.product_id = products.id
JOIN suppliers ON products.supplier_id = suppliers.id
WHERE order_items.order_id IN (3, 30)
GROUP BY MONTH(products.launch_date)
ORDER BY order_item_count DESC, launch_month DESC;
