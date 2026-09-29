SELECT order_id, COUNT(*) AS order_item_count
FROM order_items
WHERE quantity <> 3 AND quantity <> 3
GROUP BY order_id
ORDER BY order_item_count ASC, order_id ASC
LIMIT 5;
