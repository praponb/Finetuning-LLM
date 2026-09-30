WITH cleaned AS (
    SELECT
        *,
        MONTH(ends_on) AS ends_on_month,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key
    FROM promotions
    WHERE ends_on > TO_DATE('2024-01-13')
      AND channel IS NOT NULL
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    ends_on_month,
    channel_promo_code_key,
    FIRST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    AVG(ends_on_month) OVER (ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ends_on_month_avg_6_prior,
    AVG(ends_on_month) OVER (ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ends_on_month_avg_2_prior
FROM cleaned;
