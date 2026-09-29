WITH cleaned AS (
    SELECT
        *,
        CASE WHEN discount_pct >= 0.33 THEN 'High' WHEN discount_pct >= 0.19 THEN 'Medium' ELSE 'Low' END AS discount_pct_level,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM promotions
    WHERE channel IN ('mobile', 'store', 'web')
),
cleaned_promotions AS (
    SELECT
        *,
        SUBSTRING(promo_code, 1, 4) AS promo_code_prefix,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM cleaned
    WHERE starts_on >= DATE '2023-09-06'
      AND channel IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct_level,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER w_recent AS event_seq_2
    FROM cleaned_promotions
    WINDOW w_recent AS (PARTITION BY channel ORDER BY ends_on DESC, id ASC)
),
metrics AS (
    SELECT
        id,
        channel,
        first_known_channel,
        event_seq_2,
        AVG(event_seq) OVER w_hist AS channel_avg_event_seq,
        LAG(event_seq_2, 1) OVER (ORDER BY starts_on ASC, id ASC) AS prev_event_seq_2,
        NTILE(5) OVER (PARTITION BY channel ORDER BY event_seq_2 DESC, id ASC) AS event_seq_2_bucket
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY channel)
)
SELECT
    id,
    prev_event_seq_2,
    event_seq_2_bucket
FROM metrics
ORDER BY prev_event_seq_2 DESC NULLS FIRST
