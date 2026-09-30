WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, WEEKOFYEAR(launch_date) launch_date_weekofyear
    FROM products
    WHERE launch_date > DATE '2023-06-21'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IS NULL
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_sale_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty ASC NULLS FIRST, id ASC) AS dedup_rank
    FROM with_latest
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.category,
        src.price,
        src.stock_qty,
        src.tags,
        src.supplier_id,
        src.store_sale_sale_date,
        src.dedup_rank,
        p.country,
        p.rating,
        p.city
    FROM deduped src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock < s.store_sale_sale_date
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        price
    FROM kept
) src
PIVOT (
    MAX(price)
    FOR city IN ('Singapore' AS singapore, 'Bangkok' AS bangkok)
)
