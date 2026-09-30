WITH flattened AS (
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        pos + 1 AS item_no,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts t
    LATERAL VIEW OUTER POSEXPLODE(t.items) e AS pos, entry
)
SELECT
    CASE WHEN price >= 237 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    SUM(CASE WHEN promo = 'BUNDLE' THEN qty ELSE 0 END) AS bundle_qty,
    AVG(price) FILTER (WHERE promo = 'BUNDLE') AS bundle_avg_price,
    COUNT(DISTINCT id) AS distinct_ids
FROM flattened
GROUP BY price_tier
