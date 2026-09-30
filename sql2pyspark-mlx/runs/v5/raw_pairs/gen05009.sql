WITH lines AS (
    SELECT
        id,
        created_at,
        pos,
        rec.product_id,
        rec.qty,
        rec.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, rec
)
SELECT
    CASE WHEN pos >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS pos_tier,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    GROUPING_ID() AS grouping_level
FROM lines
GROUP BY CUBE (CASE WHEN pos >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY span_minutes;
