SELECT s.store_name, o.channel
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE o.order_date < '2024-07-01' OR s.city = 'Tokyo'
ORDER BY store_name;
