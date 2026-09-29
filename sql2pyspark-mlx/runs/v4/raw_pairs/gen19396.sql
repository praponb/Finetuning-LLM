SELECT
    *,
    ROW_NUMBER() OVER w_ordered event_seq,
    NTILE(4) OVER (ORDER BY cost DESC, id) cost_bucket,
    RANK() OVER (ORDER BY cost DESC NULLS LAST) cost_rank
FROM shipments
WINDOW w_ordered AS (PARTITION BY order_id, carrier ORDER BY cost DESC, id)
