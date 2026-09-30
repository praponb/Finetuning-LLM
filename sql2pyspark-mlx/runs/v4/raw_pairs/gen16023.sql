WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee, id) AS latest_rank
        FROM orders
    ) t
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.latest_rank,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'GROUP_A' WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone,
    (SELECT SUM(t2.shipping_fee) FROM orders t2 WHERE t2.store_id = s.store_id) AS sum_order_shipping_fee
FROM deduped s
ORDER BY discount DESC;
