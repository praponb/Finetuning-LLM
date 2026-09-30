SELECT
    *,
    LAG(cost, 1) OVER w_hist AS prev_cost,
    NTILE(10) OVER (PARTITION BY status, order_id ORDER BY cost DESC, id) AS cost_bucket
FROM shipments
WINDOW w_hist AS (PARTITION BY carrier ORDER BY shipped_date, id)
