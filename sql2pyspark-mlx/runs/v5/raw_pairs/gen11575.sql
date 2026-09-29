WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        DATE_TRUNC('year', ends_on) AS ends_on_period,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE starts_on > DATE '2023-07-15'
      AND channel IS DISTINCT FROM 'partner'
)
-- Window metrics over several different partitions
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    channel_clean,
    PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_percent_rank,
    ROW_NUMBER() OVER (ORDER BY ends_on_period, id) AS row_seq,
    NTILE(10) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS discount_pct_bucket
FROM base
WHERE starts_on < '2023-03-01'
