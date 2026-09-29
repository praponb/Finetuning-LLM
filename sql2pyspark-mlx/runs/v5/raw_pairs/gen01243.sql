WITH flattened AS (
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) e AS li_item
),
metrics AS (
    SELECT
        id,
        customer_id,
        qty,
        promo,
        CUME_DIST() OVER (ORDER BY qty ASC NULLS FIRST) AS qty_cume_dist,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        CUME_DIST() OVER (ORDER BY product_id ASC NULLS LAST) AS product_id_cume_dist,
        MAX(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_max_30m,
        SUM(qty) OVER (PARTITION BY customer_id) AS customer_id_sum_qty
    FROM flattened
),
metrics_carts AS (
    SELECT *, PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty_share ASC NULLS LAST) AS qty_share_percent_rank
    FROM metrics
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.qty,
        src.promo,
        src.qty_cume_dist,
        src.qty_share_percent_rank,
        d.age,
        d.city
    FROM metrics_carts src
    LEFT JOIN customers d
        ON src.customer_id = d.id
    WHERE d.is_active
)
SELECT
    id,
    qty,
    promo,
    qty_cume_dist,
    age,
    city,
    CASE WHEN city RLIKE '^(Singapore|Bangkok)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS city_zone
FROM enriched
ORDER BY promo DESC;
