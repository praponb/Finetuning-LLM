WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_discount_pct
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY channel, promo_code ORDER BY last7_avg_discount_pct) AS last7_avg_discount_pct_rank,
        FIRST_VALUE(last7_avg_discount_pct, TRUE) OVER (PARTITION BY channel ORDER BY last7_avg_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last7_avg_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS event_seq
    FROM scored
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY event_seq, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        last7_sum_discount_pct,
        last7_avg_discount_pct_rank,
        first_known_last7_avg_discount_pct,
        rn,
        SUM(last7_sum_discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY first_known_last7_avg_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last7_sum_discount_pct,
        NTILE(4) OVER (PARTITION BY promo_code ORDER BY last7_max_discount_pct DESC, id) AS last7_max_discount_pct_bucket,
        LAST_VALUE(first_known_last7_avg_discount_pct) IGNORE NULLS OVER (PARTITION BY channel ORDER BY last7_avg_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_last7_avg_discount_pct
    FROM ranked
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY last7_max_discount_pct_bucket DESC NULLS FIRST, id) AS dedup_rank
    FROM enriched_promotions
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    channel,
    CASE WHEN first_known_last7_avg_discount_pct >= 0.28 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_last7_avg_discount_pct_tier,
    COUNT_IF(promo_code = 'VIP') AS vip_events,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM ranked_promotions
GROUP BY channel, CASE WHEN first_known_last7_avg_discount_pct >= 0.28 THEN 'PREMIUM' ELSE 'STANDARD' END
