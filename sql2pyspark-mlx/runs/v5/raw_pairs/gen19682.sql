WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(region)) AS region_title
    FROM stores
    WHERE city IN ('Singapore', 'New York')
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.region_title,
        top1.order_order_date,
        top1.shipping_fee
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.region_title,
        s.order_order_date,
        s.shipping_fee,
        latest.quantity,
        latest.last_restock
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock >= s.order_order_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        shipping_fee,
        last_restock,
        LAG(quantity, 1, 0) OVER (PARTITION BY region ORDER BY order_order_date, id) AS prev_quantity,
        MIN(quantity) OVER w_recent AS region_min_quantity
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY region)
)
SELECT
    id,
    shipping_fee,
    last_restock,
    prev_quantity,
    region_min_quantity
FROM scored
