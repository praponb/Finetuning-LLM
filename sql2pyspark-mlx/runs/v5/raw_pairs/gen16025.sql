WITH matched_keys AS (
    -- Set difference on keys
    SELECT order_id, method
    FROM payments
    EXCEPT
    SELECT order_id, method
    FROM payments
    WHERE status IN ('FAILED', 'SUCCESS', 'REFUNDED')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    JOIN matched_keys k ON s.order_id = k.order_id AND s.method = k.method
)
SELECT
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    (SELECT MIN(x.quantity) FROM order_items x WHERE x.order_id = s.order_id) AS min_order_item_quantity
FROM verified s
ORDER BY method NULLS LAST;
