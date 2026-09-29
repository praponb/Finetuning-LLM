WITH joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.amount AS order_amount,
        d.channel,
        d.order_date
    FROM payments src
    INNER JOIN orders d
        ON src.order_id = d.id
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.order_amount,
    s.channel,
    s.order_date,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM joined s
ORDER BY channel DESC, paid_at;
