WITH with_last AS (
    SELECT
        s.id,
        s.region,
        s.opened_date,
        s.sqft,
        latest.order_status,
        latest.order_discount
    FROM stores s
    JOIN LATERAL (
            SELECT c.status AS order_status, c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_stores AS (
    SELECT s.*, latest.inventory_last_restock
    FROM with_last s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    SELECT s.*, top1.store_sale_units
    FROM with_last_stores s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    SELECT
        s.*,
        x.sale_date,
        x.revenue
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.product_id IN (4, 5)
        )
)
SELECT
    id,
    region,
    opened_date,
    sqft,
    order_discount,
    inventory_last_restock,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'GROUP_B' WHEN region RLIKE '^(South|East)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS region_zone
FROM not_flagged
