SELECT
    id,
    order_id,
    method,
    status
FROM payments
QUALIFY ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at DESC NULLS LAST, id) = 1
