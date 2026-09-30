WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key_2,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_channel_key,
        promo_code_channel_key_2,
        channel_clean,
        DENSE_RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct ASC) AS discount_pct_dense_rank,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct_dense_rank / SUM(discount_pct_dense_rank) OVER (PARTITION BY channel), 4) AS discount_pct_dense_rank_share,
        LAST_VALUE(discount_pct_dense_rank) IGNORE NULLS OVER (ORDER BY discount_pct_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_dense_rank
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_code_channel_key,
    promo_code_channel_key_2,
    channel_clean,
    discount_pct_dense_rank,
    discount_pct_share,
    discount_pct_dense_rank_share,
    last_known_discount_pct_dense_rank,
    STDDEV_SAMP(last_known_discount_pct_dense_rank) OVER w_grp AS promo_code_stddev_samp_last_known_discount_pct_dense_rank,
    SUM(discount_pct_dense_rank_share) OVER (PARTITION BY channel ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_dense_rank_share
FROM enriched
WINDOW w_grp AS (PARTITION BY promo_code);
