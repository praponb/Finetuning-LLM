WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        MONTH(starts_on) AS starts_on_month,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY rank_in_group, id, promo_id) AS row_num
        FROM matched
    ) x
    WHERE row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        discount_pct,
        starts_on_month,
        channel_filled,
        rank_in_group,
        promo_id,
        row_num,
        AVG(promotion_discount_pct) OVER (ORDER BY ends_on, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS promotion_discount_pct_avg_3,
        NTILE(4) OVER (PARTITION BY channel ORDER BY promotion_discount_pct, id, promo_id) AS promotion_discount_pct_bucket,
        AVG(promo_id) OVER (ORDER BY discount_pct, id, promo_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS promo_id_avg_5,
        NTILE(5) OVER w_recent AS promo_id_bucket
    FROM deduped
    WHERE discount_pct >= 0.27
    WINDOW w_recent AS (PARTITION BY channel ORDER BY promo_id, id, promo_id)
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promotion_discount_pct_bucket DESC, id, promo_id) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        discount_pct,
        channel_filled,
        rank_in_group,
        promo_id,
        row_num,
        promotion_discount_pct_avg_3,
        promotion_discount_pct_bucket,
        promo_id_avg_5,
        promo_id_bucket,
        FLOOR(promo_id_bucket) AS promo_id_bucket_whole,
        COALESCE(promotion_discount_pct_bucket, 0) AS promotion_discount_pct_bucket_filled
    FROM ranked_promotions
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promotion_discount_pct_avg_3 >= 0 THEN 'LARGE' ELSE 'SMALL' END AS promotion_discount_pct_avg_3_tier,
    promo_id,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    APPROX_PERCENTILE(discount_pct, 0.9) AS discount_pct_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    RANK() OVER (PARTITION BY CASE WHEN promotion_discount_pct_avg_3 >= 0 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(discount_pct), 2) DESC) AS avg_discount_pct_rank
FROM cleaned
GROUP BY CASE WHEN promotion_discount_pct_avg_3 >= 0 THEN 'LARGE' ELSE 'SMALL' END, promo_id
HAVING ROUND(AVG(discount_pct), 2) < 1000000
