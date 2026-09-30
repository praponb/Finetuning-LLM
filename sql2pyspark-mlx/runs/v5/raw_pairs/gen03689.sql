SELECT
    *,
    LAG(cost) OVER w_part AS prev_cost,
    LEAD(cost) OVER w_part AS next_cost,
    ROW_NUMBER() OVER w_part AS row_seq
FROM shipments
WINDOW w_part AS (PARTITION BY status, carrier ORDER BY delivered_date ASC, id ASC);
