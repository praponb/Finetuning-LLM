WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key,
        UPPER(promo_code) AS promo_code_upper
    FROM promotions
    WHERE channel IS NOT NULL
      AND promo_code IN ('SPRING10', 'VIP', 'BUNDLE')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY ends_on DESC) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(ends_on) AS ends_on_dayofweek,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM ranked
    WHERE ends_on >= DATE '2023-11-17'
      AND rn <= 1
),
filtered_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM base
    WHERE ends_on_dayofweek < 69
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM filtered_promotions
) src
PIVOT (
    SUM(discount_pct)
    FOR promo_code IN ('SPRING10', 'BUNDLE')
);
