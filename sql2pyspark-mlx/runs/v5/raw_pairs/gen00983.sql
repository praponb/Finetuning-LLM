WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE channel IS DISTINCT FROM 'partner'
)
-- Window metrics over several different partitions
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    promo_code_channel_key,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS seq_no,
    LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
    LEAD(discount_pct, 1, 0) OVER (ORDER BY discount_pct, id) AS next_discount_pct
FROM base;
