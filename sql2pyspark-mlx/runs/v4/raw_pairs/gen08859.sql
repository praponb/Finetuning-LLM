SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    NTILE(4) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
    NTILE(4) OVER w_hist AS cost_bucket_2
FROM shipments
WINDOW w_hist AS (PARTITION BY order_id ORDER BY cost DESC, id)
