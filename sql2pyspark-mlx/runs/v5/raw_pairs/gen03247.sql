WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct * 100, 2) AS discount_pct_scaled,
        LAST_DAY(ends_on) AS ends_on_month_end,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(discount_pct_scaled, TRUE) OVER (PARTITION BY promo_code ORDER BY discount_pct_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct_scaled,
        RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_rank,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share
    FROM cleaned
),
metrics_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        ends_on_month_end,
        channel_filled,
        first_known_discount_pct_scaled,
        FIRST_VALUE(discount_pct_rank) IGNORE NULLS OVER w_hist AS first_known_discount_pct_rank,
        SUM(discount_pct_share) OVER (PARTITION BY channel ORDER BY discount_pct_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_share
    FROM metrics
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY first_known_discount_pct_scaled DESC, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(first_known_discount_pct_rank) OVER (ORDER BY ends_on, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS first_known_discount_pct_rank_avg_5_prior,
        ROW_NUMBER() OVER w_running AS event_seq,
        MIN(first_known_discount_pct_scaled) OVER w_hist AS channel_min_first_known_discount_pct_scaled
    FROM latest
    WINDOW w_running AS (PARTITION BY channel ORDER BY cumulative_discount_pct_share, id), w_hist AS (PARTITION BY channel)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '7' DAY
)
-- Grouped metrics
SELECT
    promo_code,
    CASE WHEN promo_id >= 10 THEN 'HIGH' ELSE 'LOW' END AS promo_id_tier,
    ROUND(SUM(first_known_discount_pct_scaled) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS first_known_discount_pct_scaled_per_bundle,
    SUM(promo_id) AS total_promo_id,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    GROUPING(promo_code) AS grouping_level
FROM attributed
GROUP BY CUBE (promo_code, CASE WHEN promo_id >= 10 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY first_known_discount_pct_scaled_per_bundle DESC NULLS FIRST;
