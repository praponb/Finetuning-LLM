WITH lines AS (
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) li AS entry
),
enriched AS (
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) qty_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) product_id_share
    FROM lines
),
enriched_carts AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.qty,
        src.price,
        src.promo,
        src.qty_share,
        src.product_id_share,
        c.segment,
        c.city
    FROM enriched src
    JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    SELECT *, AVG(product_id_share) OVER (PARTITION BY city) city_avg_product_id_share
    FROM enriched_carts
    WHERE qty_share < 0.57
)
SELECT
    id,
    customer_id,
    created_at,
    qty,
    product_id_share,
    city,
    city_avg_product_id_share
FROM metrics
ORDER BY city_avg_product_id_share ASC NULLS LAST;
