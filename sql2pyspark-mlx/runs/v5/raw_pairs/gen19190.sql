SELECT
    *,
    LEAD(cost, 1) OVER w_ordered AS next_cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share
FROM shipments
WHERE cost >= 86
WINDOW w_ordered AS (PARTITION BY status ORDER BY shipped_date, id);
