SELECT
    p.id,
    p.status,
    o.customer_id,
    o.store_id,
    c.city,
    c.name
FROM payments p
JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY customer_id DESC, status ASC;
