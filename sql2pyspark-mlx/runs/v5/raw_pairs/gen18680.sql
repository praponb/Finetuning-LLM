WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE ends_on >= DATE '2023-05-31'
      AND channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        days_ends_on_to_starts_on,
        discount_pct_int,
        SUM(discount_pct_int) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_int,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id) AS seq_no,
        MIN(discount_pct_int) OVER (PARTITION BY channel) AS channel_min_discount_pct_int,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS row_seq
    FROM base
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '7' DAY
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_window s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        days_ends_on_to_starts_on,
        discount_pct_int,
        seq_no,
        row_seq,
        promo_id,
        discount_pct,
        promo_id_2,
        promotion_promo_code,
        LAG(discount_pct, 2) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id, promo_id_2) AS prev_discount_pct,
        MAX(discount_pct_int) OVER (PARTITION BY promotion_promo_code) AS promotion_promo_code_max_discount_pct_int,
        LAG(seq_no, 1) OVER w_recent AS prev_seq_no,
        FIRST_VALUE(promo_id, TRUE) OVER (PARTITION BY channel ORDER BY row_seq, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promo_id
    FROM matched
    WINDOW w_recent AS (ORDER BY ends_on, id, promo_id, promo_id_2)
)
SELECT
    id,
    discount_pct_int,
    row_seq,
    promo_id,
    discount_pct,
    promo_id_2,
    prev_discount_pct,
    first_known_promo_id
FROM enriched;
