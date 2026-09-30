SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    PERCENT_RANK() OVER w_grp discount_pct_percent_rank,
    NTILE(3) OVER (ORDER BY discount_pct, id) discount_pct_bucket,
    CUME_DIST() OVER w_part discount_pct_cume_dist
FROM promotions
WINDOW w_grp AS (PARTITION BY promo_code, channel ORDER BY discount_pct DESC), w_part AS (PARTITION BY channel ORDER BY discount_pct);
