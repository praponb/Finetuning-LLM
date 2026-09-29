SELECT c.segment, MIN(o.shipping_fee) AS min_shipping_fee
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
GROUP BY c.segment
HAVING MIN(o.shipping_fee) >= 23
ORDER BY min_shipping_fee, segment
LIMIT 3;
