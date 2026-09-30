WITH valid AS (
    SELECT
        *,
        CASE WHEN promo_code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-08-02')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY is_welcome5, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    SELECT
        id,
        starts_on,
        ends_on,
        promo_id,
        promotion_promo_code,
        MIN(is_welcome5) OVER (PARTITION BY promotion_promo_code ORDER BY rn, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_welcome5,
        MAX(is_welcome5) OVER (PARTITION BY promotion_promo_code ORDER BY rn, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_welcome5,
        SUM(is_welcome5) OVER (PARTITION BY promotion_promo_code ORDER BY rn, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_welcome5
    FROM with_window
)
SELECT
    id,
    promo_id,
    promotion_promo_code,
    running_sum_is_welcome5,
    LAG(running_sum_is_welcome5) OVER w_grp AS prev_running_sum_is_welcome5,
    LEAD(running_max_is_welcome5) OVER w_grp AS next_running_max_is_welcome5,
    ROW_NUMBER() OVER w_grp AS row_seq,
    SUM(running_max_is_welcome5) OVER w_hist AS promotion_promo_code_sum_running_max_is_welcome5
FROM enriched
WHERE promo_id < 9
WINDOW w_grp AS (PARTITION BY promotion_promo_code ORDER BY running_min_is_welcome5, id, promo_id), w_hist AS (PARTITION BY promotion_promo_code)
