WITH valid AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        CASE WHEN discount_pct >= 0.36 THEN 'High' WHEN discount_pct >= 0.17 THEN 'Medium' ELSE 'Low' END AS discount_pct_tier
    FROM promotions
    WHERE ends_on >= DATE '2023-07-25'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS rn
        FROM valid
    ) t
    WHERE rn = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS latest_rank
        FROM ranked
    ) t
    WHERE latest_rank = 1
),
attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 30 DAYS
)
SELECT
    id,
    channel,
    discount_pct,
    rn,
    latest_rank,
    promo_id,
    promotion_discount_pct,
    promo_id_2,
    promotion_promo_code,
    AVG(promo_id_2) OVER (ORDER BY latest_rank, id, promo_id, promo_id_2 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS promo_id_2_avg_5,
    NTILE(3) OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct, id, promo_id, promo_id_2) AS promotion_discount_pct_bucket
FROM with_window;
