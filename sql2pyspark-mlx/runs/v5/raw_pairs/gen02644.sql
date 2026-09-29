SELECT
    p.id,
    p.method,
    p.paid_at,
    o.store_id,
    o.customer_id,
    o.status,
    CASE WHEN p.method IN ('promptpay', 'cash') THEN 'Group A' ELSE 'Other' END AS method_group
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE o.customer_id IS NOT NULL;
