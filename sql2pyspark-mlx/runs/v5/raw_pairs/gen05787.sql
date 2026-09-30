WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        LEAD(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_discount_pct,
        LAG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS event_seq
    FROM promotions
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        next_discount_pct,
        prev_discount_pct,
        event_seq,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        DAY(ends_on) AS ends_on_day,
        event_seq * promo_id AS event_seq_times_promo_id
    FROM with_window
    WHERE promotion_promo_code IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY promo_code ORDER BY next_discount_pct, id, promo_id) AS next_discount_pct_bucket,
        LAST_VALUE(promo_id, TRUE) OVER (ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promo_id,
        SUM(prev_discount_pct) OVER (ORDER BY event_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_discount_pct,
        LAG(next_discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id) AS prev_next_discount_pct,
        AVG(prev_discount_pct) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_discount_pct_avg_3_prior
    FROM base
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct
    FROM metrics s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 30 DAYS
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq_times_promo_id >= 77 THEN 'HIGH' ELSE 'LOW' END AS event_seq_times_promo_id_tier,
    ROUND(SUM(event_seq_times_promo_id) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS event_seq_times_promo_id_per_spring10,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    ROUND(AVG(ends_on_day), 2) AS avg_ends_on_day
FROM matched
GROUP BY CASE WHEN event_seq_times_promo_id >= 77 THEN 'HIGH' ELSE 'LOW' END
HAVING event_seq_times_promo_id_per_spring10 < 1000000
ORDER BY event_seq_times_promo_id_tier DESC;
