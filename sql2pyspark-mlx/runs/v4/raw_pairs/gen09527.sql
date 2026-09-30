SELECT
    p.amount,
    o.order_date,
    s.store_name,
    INITCAP(TRIM(p.status)) AS status_title
FROM payments p
JOIN orders o ON o.id = p.order_id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE o.amount <= 1000 AND o.customer_id IN (33, 37);
