WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        DATE_FORMAT(starts_on, 'MMM yyyy') AS starts_on_label
    FROM promotions
    WHERE ends_on >= DATE '2023-11-29'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
ranked_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        channel_lower,
        starts_on_label,
        dedup_rank
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY dedup_rank, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(starts_on, 12) AS starts_on_plus_months,
        CONCAT(promo_code, ' - ', channel) AS promo_code_channel,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM ranked_promotions
    WHERE starts_on >= TO_DATE('2023-07-12')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(channel, TRUE) OVER w_ordered AS last_known_channel
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY promo_code, channel ORDER BY starts_on_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM with_windows a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on_plus_months >= pr.starts_on
       AND a.starts_on_plus_months < pr.ends_on + INTERVAL 30 DAYS
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(promo_id) IGNORE NULLS OVER w_ordered AS first_known_promo_id,
        MAX(promo_id) OVER (PARTITION BY promo_code, channel) AS promo_code_max_promo_id
    FROM matched
    WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    promotion_promo_code,
    promo_code_max_promo_id,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
    SUM(promo_id) AS total_promo_id,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes,
    APPROX_PERCENTILE(promo_id, 0.95) AS promo_id_p95
FROM with_windows_promotions
GROUP BY promotion_promo_code, promo_code_max_promo_id
