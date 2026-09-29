WITH enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        LAG(cost) OVER (PARTITION BY status ORDER BY shipped_date, id) prev_cost,
        LEAD(cost) OVER (PARTITION BY status ORDER BY shipped_date, id) next_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) seq_no,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost
    FROM shipments
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.prev_cost,
    s.next_cost,
    (SELECT MAX(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS max_shipment_cost
FROM enriched s
ORDER BY max_shipment_cost DESC NULLS LAST
