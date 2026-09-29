WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 10)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    MAX(promo_id) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_max_promo_id,
    COUNT_IF(channel = 'store') AS store_events,
    APPROX_PERCENTILE(promo_id, ARRAY(0.25, 0.5, 0.75)) AS promo_id_quartiles
FROM matched
GROUP BY DATE_TRUNC('WEEK', starts_on)
ORDER BY week_start
