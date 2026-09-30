SELECT
    p.id,
    p.status,
    p.method,
    o.amount,
    o.store_id,
    CONCAT_WS(',', p.method, p.status) AS method_status_key
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE p.status NOT IN ('SUCCESS', 'FAILED') AND p.order_id = 21
ORDER BY id DESC
