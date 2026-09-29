SELECT
    id,
    order_id,
    carrier,
    delivered_date,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date ASC, id ASC) AS event_seq,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS cost_percent_rank
FROM shipments;
