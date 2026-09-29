WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY discount_pct, id) AS prev_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC) AS discount_pct_percent_rank,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2
    FROM promotions
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct_avg_6_prior,
    prev_discount_pct,
    CASE WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS promo_code_zone
FROM scored
ORDER BY promo_code NULLS FIRST
