WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', starts_on) AS starts_on_period,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE starts_on > TO_DATE('2023-08-29')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on_period >= pr.starts_on
       AND e.starts_on_period < pr.ends_on + INTERVAL 30 DAYS
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_period,
        resolved_channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        FIRST_VALUE(promo_id, TRUE) OVER (PARTITION BY channel, promo_code ORDER BY promotion_discount_pct ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promo_id,
        LAST_VALUE(promotion_discount_pct, TRUE) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promotion_discount_pct
    FROM attributed
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(starts_on_period) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on_period ASC, id ASC, promo_id ASC) AS prev_starts_on_period
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_starts_on_period IS NULL OR starts_on_period > prev_starts_on_period + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_promotions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on_period ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM sessions_promotions a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC, promo_id_2 ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_pct_avg_3,
        LAG(promotion_discount_pct_2, 1) OVER (PARTITION BY promotion_promo_code_2 ORDER BY discount_pct ASC, id ASC, promo_id ASC, promo_id_2 ASC) AS prev_promotion_discount_pct_2
    FROM attributed_promotions
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM enriched src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on_period BETWEEN pr.starts_on AND pr.ends_on
)
-- Multi-level aggregation
SELECT
    promotion_promo_code_2,
    PERCENTILE_APPROX(discount_pct_avg_3, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_avg_3_quartiles,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    MIN(promotion_discount_pct) AS min_promotion_discount_pct
FROM with_window
GROUP BY promotion_promo_code_2
