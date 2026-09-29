WITH filtered AS (
    SELECT *, INITCAP(TRIM(promo_code)) promo_code_title
    FROM promotions
    WHERE ends_on >= DATE '2023-08-13'
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_title,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on, id) event_seq,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) discount_pct_dense_rank,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct
    FROM filtered
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_title,
        event_seq,
        discount_pct_dense_rank,
        cumulative_discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        SUM(event_seq) OVER (PARTITION BY promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_event_seq,
        NTILE(3) OVER (PARTITION BY channel ORDER BY discount_pct_dense_rank, id, promo_id) discount_pct_dense_rank_bucket,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id, id, promo_id) row_seq,
        NTILE(4) OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id, promo_id) discount_pct_bucket
    FROM matched
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY promo_id, id, promo_id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM ranked src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 7 DAYS
)
SELECT
    id,
    ends_on,
    discount_pct,
    promo_code_title,
    cumulative_discount_pct,
    promo_id,
    promotion_promo_code,
    row_seq,
    promo_id_2
FROM attributed
ORDER BY ends_on
