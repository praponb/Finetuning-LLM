WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        NTILE(5) OVER (ORDER BY discount_pct, id) AS discount_pct_bucket,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) AS event_seq,
        LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM promotions
    WHERE YEAR(ends_on) = 2025
)
SELECT
    id,
    channel,
    discount_pct,
    discount_pct_bucket,
    event_seq,
    prev_discount_pct,
    first_known_channel,
    CASE WHEN channel RLIKE '^(store|partner)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS channel_zone
FROM scored
ORDER BY discount_pct_bucket NULLS LAST;
