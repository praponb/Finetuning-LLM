WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        DATE_SUB(starts_on, 54) AS starts_on_minus_54d,
        CONCAT_WS('/', channel, promo_code) AS channel_promo_code_key
    FROM promotions
    WHERE starts_on > DATE '2023-08-07'
      AND channel IS NOT NULL
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) = 1
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY starts_on_minus_54d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_cume_dist_2
    FROM deduped
),
base AS (
    SELECT *, CONCAT(promo_code, ', ', channel) AS promo_code_channel
    FROM metrics
    WHERE channel IS NOT NULL
      AND discount_pct >= 0.15
),
latest AS (
    SELECT
        id,
        discount_pct,
        channel_promo_code_key,
        discount_pct_cume_dist,
        last_known_channel,
        discount_pct_cume_dist_2,
        promo_code_channel
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) = 1
)
SELECT
    id,
    discount_pct,
    channel_promo_code_key,
    discount_pct_cume_dist,
    last_known_channel,
    promo_code_channel
FROM latest
ORDER BY id NULLS FIRST
