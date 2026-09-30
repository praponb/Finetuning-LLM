WITH filtered AS (
    SELECT
        *,
        TRIM(channel) AS clean_channel,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE order_date >= TO_DATE('2023-08-15')
      AND customer_id IS NOT NULL
)
SELECT s.*, top1.shipped_date
FROM filtered s
JOIN LATERAL (
        SELECT c.shipped_date
        FROM shipments c
        WHERE c.order_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
