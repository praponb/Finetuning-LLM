WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        DATE_ADD(ends_on, 65) AS ends_on_plus_65d
    FROM promotions
    WHERE starts_on > DATE '2023-08-01'
      AND channel IN ('store', 'partner')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on_plus_65d >= pr.starts_on
       AND s.ends_on_plus_65d < pr.ends_on + INTERVAL '14' DAY
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_code,
        ends_on_plus_65d,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on_plus_65d ASC, id ASC, promo_id ASC) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct
    FROM deduped a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on_plus_65d >= pr.starts_on
       AND a.ends_on_plus_65d <= DATE_ADD(pr.ends_on, 10)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(discount_pct) OVER (PARTITION BY promotion_promo_code_2, promotion_promo_code ORDER BY promo_id_2 ASC, id ASC, promo_id ASC, promo_id_2 ASC) AS prev_discount_pct,
        LAG(promo_id) OVER (PARTITION BY promotion_promo_code_2, promotion_promo_code ORDER BY promo_id_2 ASC, id ASC, promo_id ASC, promo_id_2 ASC) AS prev_promo_id,
        LEAD(promo_id) OVER (PARTITION BY promotion_promo_code_2, promotion_promo_code ORDER BY promo_id_2 ASC, id ASC, promo_id ASC, promo_id_2 ASC) AS next_promo_id,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code_2, promotion_promo_code ORDER BY promo_id_2 ASC, id ASC, promo_id ASC, promo_id_2 ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY promo_id_2 DESC, id ASC, promo_id ASC, promo_id_2 ASC) AS seq_no_2
    FROM matched
),
deduped_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on_plus_65d DESC NULLS LAST, id ASC, promo_id ASC, promo_id_2 ASC) = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', ends_on_plus_65d) AS activity_month,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS discount_pct_per_web,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    PERCENTILE_APPROX(next_promo_id, 0.5) AS next_promo_id_p50,
    SUM(CASE WHEN promotion_promo_code = 'BUNDLE' THEN prev_discount_pct ELSE 0 END) AS bundle_prev_discount_pct
FROM deduped_promotions
GROUP BY DATE_TRUNC('MONTH', ends_on_plus_65d)
