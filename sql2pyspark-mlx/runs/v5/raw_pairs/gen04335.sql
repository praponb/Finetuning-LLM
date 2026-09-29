WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        ROUND(discount_pct * 1.07, 2) AS discount_pct_adjusted,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned e
    INNER JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '14' DAY
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '7' DAY
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY promotion_discount_pct_2, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        ROW_NUMBER() OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY discount_pct_adjusted, id, promo_id, promo_id_2) AS seq_no
    FROM matched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(promotion_promo_code, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promotion_promo_code,
        NTILE(5) OVER (ORDER BY days_ends_on_to_starts_on, id, promo_id, promo_id_2) AS days_ends_on_to_starts_on_bucket
    FROM scored
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id_2, id, promo_id, promo_id_2) AS rn
    FROM metrics
    QUALIFY rn = 1
),
metrics_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        days_ends_on_to_starts_on,
        resolved_channel,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        last_known_channel,
        seq_no,
        last_known_promotion_promo_code,
        rn,
        LAG(promo_id, 2) OVER (PARTITION BY promotion_promo_code_2, promo_code ORDER BY rn, id, promo_id, promo_id_2) AS prev_promo_id,
        SUM(promotion_discount_pct_2) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promotion_discount_pct_2
    FROM latest
    WHERE promotion_promo_code NOT IN ('VIP', 'WELCOME5', 'SPRING10')
),
totals AS (
    -- Grouped metrics
    SELECT
        channel,
        ROUND(SUM(prev_promo_id) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS prev_promo_id_per_partner,
        ROUND(AVG(promotion_discount_pct), 2) AS avg_promotion_discount_pct,
        COUNT(*) AS cnt,
        COUNT(*) AS event_count,
        COUNT_IF(promotion_promo_code_2 = 'SPRING10') AS spring10_events
    FROM metrics_promotions
    GROUP BY channel
)
SELECT
    channel,
    prev_promo_id_per_partner,
    avg_promotion_discount_pct,
    cnt,
    event_count
FROM totals
ORDER BY prev_promo_id_per_partner DESC NULLS LAST
