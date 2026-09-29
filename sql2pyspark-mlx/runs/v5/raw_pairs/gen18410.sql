SELECT
    id,
    status,
    discount,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) AS rn
FROM orders
WHERE customer_id IN (8, 30, 37)
