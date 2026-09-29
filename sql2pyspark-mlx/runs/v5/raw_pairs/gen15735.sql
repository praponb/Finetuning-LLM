WITH scored AS (
    SELECT
        *,
        SUM(discount_pct) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        NTILE(5) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS discount_pct_bucket
    FROM promotions
),
metrics AS (
    SELECT
        *,
        LEAD(discount_pct_avg_2_prior) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS next_discount_pct_avg_2_prior,
        LEAD(cumulative_discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS next_cumulative_discount_pct,
        LAG(cumulative_discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS prev_cumulative_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS event_seq
    FROM scored
),
enriched AS (
    SELECT
        *,
        ROUND(discount_pct_bucket / SUM(discount_pct_bucket) OVER (PARTITION BY channel), 4) AS discount_pct_bucket_share,
        ROUND(prev_cumulative_discount_pct / SUM(prev_cumulative_discount_pct) OVER (PARTITION BY channel), 4) AS prev_cumulative_discount_pct_share,
        AVG(running_discount_pct_2) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_discount_pct_2_avg_6
    FROM metrics
),
valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        running_discount_pct,
        running_discount_pct_2,
        cumulative_discount_pct,
        discount_pct_bucket,
        prev_cumulative_discount_pct,
        event_seq,
        discount_pct_bucket_share,
        prev_cumulative_discount_pct_share,
        prev_cumulative_discount_pct + discount_pct AS prev_cumulative_discount_pct_plus_discount_pct
    FROM enriched
    WHERE starts_on > DATE '2023-04-09'
),
matched AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid a
    INNER JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
metrics_promotions AS (
    SELECT
        *,
        SUM(event_seq) OVER (ORDER BY event_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq,
        LEAD(prev_cumulative_discount_pct, 2) OVER (PARTITION BY channel ORDER BY discount_pct_bucket, id, promo_id) AS next_prev_cumulative_discount_pct
    FROM matched
),
summary AS (
    SELECT
        promo_code,
        DATE_TRUNC('MONTH', starts_on) AS activity_month,
        COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
        COUNT(CASE WHEN promotion_promo_code = 'VIP' THEN 1 END) AS vip_count,
        COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count
    FROM metrics_promotions
    GROUP BY GROUPING SETS ((promo_code, DATE_TRUNC('MONTH', starts_on)), (promo_code), ())
)
SELECT promo_code, welcome5_count
FROM summary
ORDER BY promo_code DESC NULLS LAST
