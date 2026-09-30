SELECT
    id,
    method,
    status,
    paid_at,
    ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at, id) AS rn
FROM payments
ORDER BY id DESC;
