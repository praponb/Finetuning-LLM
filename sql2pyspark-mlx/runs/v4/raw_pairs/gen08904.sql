SELECT customers.country, MIN(shipments.delivered_date) AS min_delivered_date, SORT_ARRAY(COLLECT_SET(shipments.carrier)) AS carrier_list
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
INNER JOIN customers ON customers.id = orders.customer_id
GROUP BY customers.country
ORDER BY customers.country, min_delivered_date
LIMIT 5;
