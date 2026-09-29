WITH flattened AS (
    SELECT
        id,
        created_at,
        pos,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) x AS pos, line
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    SUM(CASE WHEN promo = 'BUNDLE' THEN price ELSE 0 END) AS bundle_price,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    SUM(CASE WHEN promo = 'SPRING10' THEN price ELSE 0 END) AS spring10_price
FROM flattened
GROUP BY 1
HAVING SUM(CASE WHEN promo = 'SPRING10' THEN price ELSE 0 END) < 1000000
ORDER BY bundle_price;
