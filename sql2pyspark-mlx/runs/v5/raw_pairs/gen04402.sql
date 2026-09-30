WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        starts_on,
        INITCAP(TRIM(channel)) AS channel_title
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on > DATE '2023-07-16'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ROW_NUMBER() OVER w_grp AS row_seq,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM cleaned
    WHERE starts_on > '2024-09-01'
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY starts_on DESC, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq DESC NULLS FIRST, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY seq_no DESC NULLS FIRST, id) AS rn
    FROM deduped
    QUALIFY rn = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq NULLS LAST, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    seq_no,
    CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    COUNT_IF(promo_code = 'WELCOME5') AS welcome5_events,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM deduped_promotions
GROUP BY seq_no, CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY row_seq_tier NULLS FIRST
