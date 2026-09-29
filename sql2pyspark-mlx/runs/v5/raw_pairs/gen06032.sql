WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, order_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS LAST, id) AS rn
        FROM order_items
    ) t
    WHERE rn = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        p.shipping_fee,
        p.status
    FROM deduped a
    JOIN orders p
        ON a.order_id = p.id
    WHERE p.amount BETWEEN 510 AND 1330
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        status,
        LEAD(shipping_fee) OVER (PARTITION BY order_id ORDER BY shipping_fee, id) AS next_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipping_fee, id) AS seq_no
    FROM with_dims
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY seq_no DESC, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.status,
    s.next_shipping_fee,
    s.seq_no,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'CORE' WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS status_zone,
    (SELECT MIN(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS min_payment_amount
FROM ranked s
ORDER BY status_zone;
