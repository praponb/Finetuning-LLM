WITH scored AS (
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        AVG(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        MAX(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_max_discount_pct
    FROM promotions
)
SELECT
    channel,
    promo_code,
    CASE WHEN discount_pct >= 0.3 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    ROUND(SUM(promo_code_max_discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS promo_code_max_discount_pct_per_bundle,
    SUM(discount_pct_avg_3_prior) FILTER (WHERE channel = 'partner') AS partner_total_discount_pct_avg_3_prior
FROM scored
GROUP BY CUBE (channel, promo_code, CASE WHEN discount_pct >= 0.3 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY promo_code DESC;
