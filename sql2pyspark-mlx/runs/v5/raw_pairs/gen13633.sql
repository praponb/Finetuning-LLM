SELECT
    id,
    carrier,
    delivered_date,
    cost,
    status,
    LEAD(cost) OVER w_hist AS next_cost,
    ROW_NUMBER() OVER w_hist AS seq_no,
    NTILE(4) OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_bucket
FROM shipments
WINDOW w_hist AS (PARTITION BY status ORDER BY delivered_date, id);
