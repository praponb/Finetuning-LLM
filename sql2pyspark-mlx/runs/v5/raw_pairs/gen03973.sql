WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount) AS row_num
        FROM payments
    ) ranked
    WHERE row_num = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        p.amount AS order_amount,
        p.order_date,
        p.status
    FROM deduped a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.order_date > '2023-04-06'
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, o.discount
    FROM with_dims src
    INNER JOIN orders o
        ON src.order_id <=> o.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY order_amount DESC, id) AS latest_rank
        FROM joined
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    order_amount,
    order_date,
    status,
    discount,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM ranked
ORDER BY order_date DESC, status DESC
