WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        TRIM(promo_code) AS clean_promo_code,
        CASE WHEN discount_pct >= 0.3 THEN 'High' WHEN discount_pct >= 0.16 THEN 'Medium' ELSE 'Low' END AS discount_pct_level,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on >= DATE '2023-10-08'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(discount_pct) OVER (PARTITION BY channel ORDER BY dedup_rank, id) AS next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY dedup_rank, id) AS row_seq
    FROM ranked
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        resolved_channel,
        dedup_rank,
        next_discount_pct,
        row_seq,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_avg_5,
        AVG(row_seq) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_avg_2_prior,
        AVG(discount_pct) OVER (PARTITION BY promo_code, channel) AS promo_code_avg_discount_pct,
        LEAD(row_seq, 1) OVER (PARTITION BY channel ORDER BY dedup_rank, id) AS next_row_seq
    FROM enriched
    WHERE next_discount_pct <= 0.25
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY next_discount_pct DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    ROUND(AVG(promo_code_avg_discount_pct), 2) AS avg_promo_code_avg_discount_pct,
    ROUND(SUM(discount_pct_avg_5) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS discount_pct_avg_5_per_bundle,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM deduped
GROUP BY row_seq_tier
