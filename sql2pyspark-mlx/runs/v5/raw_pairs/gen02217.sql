WITH deduped AS (
    SELECT
        id,
        channel,
        discount_pct
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id) = 1
),
ranked AS (
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY discount_pct) = 1
),
scored AS (
    SELECT
        *,
        SUM(discount_pct) OVER w_hist cumulative_discount_pct,
        NTILE(10) OVER (PARTITION BY channel ORDER BY discount_pct, id) discount_pct_bucket,
        MIN(discount_pct) OVER w_grp channel_min_discount_pct,
        FIRST_VALUE(channel, TRUE) OVER w_hist first_known_channel,
        LAST_VALUE(channel) IGNORE NULLS OVER w_hist last_known_channel
    FROM ranked
    WHERE discount_pct >= 0.32
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY channel)
),
with_windows AS (
    SELECT
        *,
        ROUND(channel_min_discount_pct / SUM(channel_min_discount_pct) OVER (PARTITION BY channel), 4) channel_min_discount_pct_share,
        AVG(discount_pct_bucket) OVER (ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_pct_bucket_avg_2,
        NTILE(4) OVER (PARTITION BY channel ORDER BY cumulative_discount_pct, id) cumulative_discount_pct_bucket,
        AVG(discount_pct_bucket) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) discount_pct_bucket_avg_5,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) event_seq
    FROM scored
),
deduped_promotions AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY channel_min_discount_pct_share, id) = 1
),
filtered AS (
    SELECT
        *,
        discount_pct_bucket * channel_min_discount_pct_share discount_pct_bucket_times_channel_min_discount_pct_share,
        CAST(discount_pct_bucket AS STRING) discount_pct_bucket_str,
        COALESCE(channel, 'NONE') resolved_channel
    FROM deduped_promotions
    WHERE discount_pct_bucket_avg_5 = 4
)
SELECT
    channel,
    COUNT_IF(channel = 'partner') AS partner_events,
    PERCENTILE_APPROX(discount_pct_bucket_avg_5, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_bucket_avg_5_quartiles,
    COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count,
    ROUND(SUM(channel_min_discount_pct_share) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS channel_min_discount_pct_share_per_mobile,
    PERCENTILE_APPROX(channel_min_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS channel_min_discount_pct_quartiles
FROM filtered
GROUP BY 1
ORDER BY channel;
