SELECT o.channel, s.shipped_date
FROM orders o
INNER JOIN shipments s ON s.order_id = o.id
WHERE s.carrier = 'UPS' OR o.amount < 1550
ORDER BY s.shipped_date;
