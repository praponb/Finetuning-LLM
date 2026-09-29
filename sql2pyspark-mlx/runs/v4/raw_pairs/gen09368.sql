SELECT
    status,
    order_id,
    paid_at,
    INITCAP(TRIM(method)) AS method_title
FROM payments
WHERE amount BETWEEN 810 AND 1440 AND order_id IN (4, 15, 28);
