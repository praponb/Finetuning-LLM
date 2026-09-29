WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, YEAR(starts_on) starts_on_year
    FROM promotions
    WHERE MONTH(starts_on) IN (5, 6, 11)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY starts_on_year NULLS FIRST) starts_on_year_dense_rank,
        SUM(starts_on_year) OVER w_grp running_starts_on_year,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) seq_no
    FROM filtered
    WINDOW w_grp AS (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
filtered_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(ends_on, 'yyyy-MM') ends_on_label,
        starts_on_year_dense_rank + seq_no starts_on_year_dense_rank_plus_seq_no
    FROM with_window
    WHERE starts_on >= DATE '2023-04-11'
      AND channel IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct DESC NULLS FIRST, id, promo_id) AS dedup_rank
    FROM filtered_promotions
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    starts_on_year,
    starts_on_year_dense_rank,
    running_starts_on_year,
    promo_id,
    promotion_discount_pct,
    CASE WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'EXTENDED' WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'CORE' ELSE 'GROUP_A' END AS promo_code_zone
FROM ranked
ORDER BY starts_on NULLS FIRST
