WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
)
-- Aggregate with conditional metrics
SELECT
    promo,
    COUNT_IF(promo = 'VIP') AS vip_events,
    COUNT_IF(promo = 'SPRING10') AS spring10_events,
    COUNT_IF(promo = 'VIP') AS vip_events_2,
    COUNT(*) AS num_rows
FROM lines
GROUP BY promo
HAVING COUNT_IF(promo = 'SPRING10') < 1000;
