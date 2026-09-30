WITH enriched AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_pct_avg_3,
        RANK() OVER w_grp AS discount_pct_rank
    FROM promotions
    WINDOW w_grp AS (PARTITION BY channel ORDER BY discount_pct DESC)
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_avg_3,
    discount_pct_rank,
    CASE WHEN promo_code RLIKE '^(SPRING10|WELCOME5)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS promo_code_zone
FROM enriched
ORDER BY discount_pct DESC NULLS LAST
