WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount DESC, id) AS event_seq,
        LAST_VALUE(method, TRUE) OVER w_grp AS last_known_method,
        AVG(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM payments
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.event_seq,
    s.last_known_method,
    CASE WHEN method RLIKE '^(promptpay|cash)$' THEN 'GROUP_B' WHEN method RLIKE '^(card|paypal)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS method_zone,
    (SELECT AVG(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS avg_shipment_cost
FROM scored s
ORDER BY method_zone DESC NULLS FIRST;
