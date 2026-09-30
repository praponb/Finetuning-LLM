WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS latest_rank
        FROM sessions
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    (SELECT MAX(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_shipping_fee
FROM ranked s;
