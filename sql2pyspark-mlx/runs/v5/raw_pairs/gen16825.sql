WITH scored AS (
    SELECT
        *,
        LAG(cost, 1) OVER (PARTITION BY order_id ORDER BY cost, id) AS prev_cost,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY shipped_date, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY delivered_date DESC, id) AS event_seq,
        AVG(cost) OVER (PARTITION BY status, carrier ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5
    FROM shipments
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.row_seq,
    s.cost_avg_5,
    CASE WHEN carrier RLIKE '^(FedEx|Kerry)$' THEN 'PRIORITY' WHEN carrier RLIKE '^(DHL|Flash)$' THEN 'GROUP_A' ELSE 'CORE' END AS carrier_zone,
    (SELECT MIN(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS min_payment_amount
FROM scored s
