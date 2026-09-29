WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount_pct) OVER w_hist AS running_discount_pct,
        AVG(discount_pct) OVER w_running AS discount_pct_avg_5
    FROM promotions
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    running_discount_pct,
    discount_pct_avg_5
FROM scored
ORDER BY running_discount_pct, ends_on
