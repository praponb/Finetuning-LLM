WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key,
        ROUND(discount_pct, 2) AS discount_pct_rounded
    FROM promotions
    WHERE ends_on > TO_DATE('2024-04-18')
      AND channel IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_cume_dist
    FROM valid
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        ends_on,
        event_seq,
        discount_pct_cume_dist,
        DATE_ADD(ends_on, 77) AS ends_on_plus_77d
    FROM with_windows
    WHERE ends_on > TO_DATE('2024-04-22')
      AND starts_on >= '2023-09-01'
),
with_windows_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount_pct_cume_dist) OVER (ORDER BY ends_on_plus_77d, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_cume_dist_avg_6_prior,
        LAG(discount_pct_cume_dist, 2) OVER w_running AS prev_discount_pct_cume_dist,
        FIRST_VALUE(event_seq, TRUE) OVER (ORDER BY ends_on_plus_77d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        LEAD(event_seq, 2) OVER (ORDER BY ends_on, id) AS next_event_seq,
        NTILE(10) OVER w_running AS event_seq_bucket
    FROM filtered
    WINDOW w_running AS (ORDER BY event_seq, id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(prev_discount_pct_cume_dist, 0) AS prev_discount_pct_cume_dist_rounded
    FROM with_windows_promotions
    WHERE ends_on > DATE '2023-08-07'
      AND prev_discount_pct_cume_dist < 0.38
)
SELECT
    id,
    ends_on,
    event_seq,
    discount_pct_cume_dist,
    discount_pct_cume_dist_avg_6_prior,
    prev_discount_pct_cume_dist,
    next_event_seq,
    event_seq_bucket,
    prev_discount_pct_cume_dist_rounded
FROM cleaned
ORDER BY event_seq_bucket NULLS FIRST
