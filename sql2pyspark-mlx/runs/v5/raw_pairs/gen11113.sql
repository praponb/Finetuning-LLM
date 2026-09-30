SELECT orders.channel, ROUND(STDDEV(order_items.unit_price), 2) AS stddev_unit_price, COUNT(*) AS order_item_count
FROM order_items
LEFT OUTER JOIN products ON products.id = order_items.product_id
JOIN orders ON orders.id = order_items.order_id
GROUP BY orders.channel
ORDER BY channel;
