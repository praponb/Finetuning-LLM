WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        delivered_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY cost DESC, id) AS rn
        FROM shipments
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        d.order_date
    FROM deduped e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC, id) row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date, id) seq_no
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.row_seq_2,
        e.seq_no,
        b.status,
        b.amount
    FROM metrics e
    INNER JOIN orders b
        ON e.order_id = b.id
    WHERE b.discount <= 0.22
)
SELECT
    id,
    row_seq_2,
    seq_no,
    status,
    amount,
    CASE WHEN status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS status_zone
FROM joined
