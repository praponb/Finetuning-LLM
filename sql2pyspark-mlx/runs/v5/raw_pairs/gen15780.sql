SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost,
    LAG(cost, 1) OVER w_grp AS prev_cost
FROM shipments
WHERE shipped_date <= '2025-02-01'
WINDOW w_grp AS (PARTITION BY order_id, status ORDER BY shipped_date, id)
