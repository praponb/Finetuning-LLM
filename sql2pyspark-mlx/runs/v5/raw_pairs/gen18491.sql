WITH filtered AS (
    SELECT
        *,
        DATE_SUB(starts_on, 6) AS starts_on_minus_6d,
        UPPER(channel) AS channel_upper
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on >= '2025-10-01'
)
SELECT
    id,
    starts_on,
    ends_on,
    channel_upper,
    FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on_minus_6d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
FROM filtered;
