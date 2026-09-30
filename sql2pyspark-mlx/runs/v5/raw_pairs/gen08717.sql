WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CONCAT(promo_code, '/', channel) AS promo_code_channel
    FROM promotions
    WHERE starts_on > DATE '2023-08-08'
      AND channel IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        promo_code_channel,
        SUM(discount_pct) OVER w_grp AS last4_sum_discount_pct,
        MAX(discount_pct) OVER w_grp AS last4_max_discount_pct,
        COUNT(discount_pct) OVER w_grp AS last4_count_discount_pct
    FROM valid
    WHERE promo_code IN ('SPRING10', 'WELCOME5')
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        promo_code,
        last4_sum_discount_pct,
        last4_max_discount_pct,
        last4_count_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY last4_count_discount_pct DESC, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
enriched_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        last4_sum_discount_pct,
        NTILE(4) OVER w_ordered AS last4_sum_discount_pct_bucket,
        ROUND(last4_sum_discount_pct / SUM(last4_sum_discount_pct) OVER (PARTITION BY promo_code), 4) AS last4_sum_discount_pct_share,
        LEAD(last4_max_discount_pct, 2) OVER (ORDER BY last4_count_discount_pct, id) AS next_last4_max_discount_pct
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY last4_sum_discount_pct, id)
)
-- Aggregate with conditional metrics
SELECT
    promo_code,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    MAX(last4_sum_discount_pct) AS max_last4_sum_discount_pct,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    ROUND(AVG(last4_sum_discount_pct_bucket), 2) AS avg_last4_sum_discount_pct_bucket
FROM enriched_promotions
GROUP BY promo_code
ORDER BY avg_last4_sum_discount_pct_bucket DESC;
