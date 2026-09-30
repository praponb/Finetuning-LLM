WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle
    FROM promotions
    WHERE channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        ends_on,
        discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        RANK() OVER w_ordered AS discount_pct_rank,
        SUM(is_bundle) OVER w_running AS cumulative_is_bundle,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC, id ASC) AS event_seq,
        SUM(discount_pct) OVER w_running AS cumulative_discount_pct_2
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY discount_pct DESC), w_running AS (PARTITION BY channel ORDER BY is_bundle ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        discount_pct,
        discount_pct_rank,
        cumulative_is_bundle,
        event_seq,
        cumulative_discount_pct_2,
        ROUND(cumulative_is_bundle / discount_pct_rank, 2) AS cumulative_is_bundle_per_discount_pct_rank,
        COALESCE(event_seq, 0) AS event_seq_filled
    FROM metrics
    WHERE ends_on > TO_DATE('2023-10-20')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ends_on,
        discount_pct,
        cumulative_is_bundle,
        event_seq,
        cumulative_is_bundle_per_discount_pct_rank,
        event_seq_filled,
        SUM(cumulative_is_bundle_per_discount_pct_rank) OVER (ORDER BY cumulative_discount_pct_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_is_bundle_per_discount_pct_rank,
        NTILE(3) OVER (ORDER BY event_seq ASC, id ASC) AS event_seq_bucket,
        AVG(cumulative_is_bundle_per_discount_pct_rank) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_is_bundle_per_discount_pct_rank_avg_6
    FROM filtered
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        discount_pct,
        cumulative_is_bundle,
        event_seq,
        event_seq_filled,
        cumulative_cumulative_is_bundle_per_discount_pct_rank,
        cumulative_is_bundle_per_discount_pct_rank_avg_6,
        CAST(cumulative_is_bundle_per_discount_pct_rank_avg_6 AS STRING) AS cumulative_is_bundle_per_discount_pct_rank_avg_6_num
    FROM with_windows
    WHERE ends_on > DATE '2023-11-01'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        event_seq_filled,
        discount_pct
    FROM valid
) src
UNPIVOT (
    metric_value FOR metric IN (event_seq_filled, discount_pct)
)
