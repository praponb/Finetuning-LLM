SELECT
    *,
    ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
    AVG(cost) OVER (PARTITION BY carrier, order_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2,
    LAST_VALUE(carrier) IGNORE NULLS OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
    MAX(cost) OVER (PARTITION BY status, order_id) AS status_max_cost,
    ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY shipped_date DESC, id) AS event_seq
FROM shipments;
