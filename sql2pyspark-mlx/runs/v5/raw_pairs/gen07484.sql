WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN discount_pct >= 0.35 THEN 'High' WHEN discount_pct >= 0.16 THEN 'Medium' ELSE 'Low' END AS discount_pct_level
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-08-12')
      AND promo_code IN ('SPRING10', 'BUNDLE')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        LEAD(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS event_seq
    FROM base
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY next_discount_pct NULLS FIRST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM ranked e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 7 DAYS
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_id DESC NULLS FIRST, id, promo_id) AS rn
        FROM matched
    ) t
    WHERE rn = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            promotion_promo_code,
            promo_code,
            channel,
            next_discount_pct
        FROM ranked_promotions
    ) src
    PIVOT (
        SUM(next_discount_pct) AS amount
        FOR channel IN ('web', 'mobile')
    )
)
SELECT
    promotion_promo_code,
    promo_code,
    web
FROM wide;
