WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(price) AS price_whole
    FROM products
    WHERE brand IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Stark'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.brand,
        a.price,
        a.stock_qty,
        a.tags,
        a.supplier_id,
        p.rating,
        p.supplier_name,
        p.city
    FROM cleaned a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.price,
        s.tags,
        s.supplier_id,
        s.rating,
        latest.inventory_last_restock
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (3, 8, 9, 10)
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(price) OVER w_recent AS brand_sum_price,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY brand)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY rating DESC, id) = 1
),
latest_products AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY brand_sum_price DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
)
SELECT
    s.id,
    s.brand,
    s.price,
    s.tags,
    s.supplier_id,
    s.rating,
    s.inventory_last_restock,
    s.brand_sum_price,
    s.price_share,
    (SELECT SUM(t2.stock_qty) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM latest_products s
ORDER BY price_share DESC
