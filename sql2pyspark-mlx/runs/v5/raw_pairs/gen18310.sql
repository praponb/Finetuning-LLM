WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(discount_pct) OVER w_grp AS running_max_discount_pct,
        COUNT(discount_pct) OVER w_grp AS running_count_discount_pct,
        SUM(discount_pct) OVER w_grp AS running_sum_discount_pct,
        AVG(discount_pct) OVER w_grp AS running_avg_discount_pct,
        ROW_NUMBER() OVER w_grp_2 AS seq_no
    FROM promotions
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 10)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        discount_pct * promotion_discount_pct AS discount_pct_times_promotion_discount_pct,
        discount_pct + promo_id AS discount_pct_plus_promo_id
    FROM matched
    WHERE ends_on >= TO_DATE('2023-12-04')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_id,
        promotion_promo_code,
        discount_pct_times_promotion_discount_pct,
        FIRST_VALUE(running_sum_discount_pct, TRUE) OVER (ORDER BY running_avg_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_discount_pct,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct_times_promotion_discount_pct DESC) AS discount_pct_times_promotion_discount_pct_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY running_max_discount_pct DESC, id, promo_id) AS row_seq
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    promo_id,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS row_seq_per_mobile,
    COUNT(CASE WHEN promotion_promo_code = 'VIP' THEN 1 END) AS vip_count,
    AVG(promo_id) FILTER (WHERE channel = 'web') AS web_avg_promo_id
FROM scored
GROUP BY promo_id
ORDER BY vip_count NULLS LAST;
