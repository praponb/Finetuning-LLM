WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ARRAY_JOIN(tags, ', ') tags_text
    FROM products
    WHERE brand IS NOT NULL
      AND price < 430
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.units,
        top1.sale_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.units, c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) = 1
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.inventory_quantity
    FROM deduped s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.sale_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (10, 17, 26, 39)
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        brand,
        price,
        cost
    FROM eligible
) src
UNPIVOT (
    value FOR metric_name IN (price, cost)
);
