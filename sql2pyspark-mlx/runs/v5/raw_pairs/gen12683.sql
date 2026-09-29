WITH lines AS (
    SELECT
        id,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS elem
)
SELECT
    id,
    product_id,
    CUME_DIST() OVER (ORDER BY qty DESC NULLS LAST) AS qty_cume_dist,
    RANK() OVER (PARTITION BY promo ORDER BY qty NULLS LAST) AS qty_rank
FROM lines
