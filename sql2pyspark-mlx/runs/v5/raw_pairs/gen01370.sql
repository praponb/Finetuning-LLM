WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        li_item.product_id,
        li_item.promo
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) e AS li_item
)
SELECT id
FROM exploded
ORDER BY id DESC NULLS FIRST;
