WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(paid_at) paid_at_hour
    FROM payments
    WHERE order_id IS NOT NULL
)
-- Attach orders attributes
SELECT
    a.*,
    p.order_date,
    p.status AS order_status,
    p.amount AS order_amount
FROM valid a
LEFT JOIN orders p
    ON a.order_id = p.id;
