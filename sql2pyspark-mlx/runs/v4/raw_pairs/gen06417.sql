WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        CASE WHEN discount_pct >= 0.3 THEN 'High' WHEN discount_pct >= 0.18 THEN 'Medium' ELSE 'Low' END AS discount_pct_level,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2024-03-15'
      AND channel IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY ends_on ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY ends_on DESC, id ASC) AS event_seq_2,
        ROW_NUMBER() OVER (ORDER BY starts_on ASC, id ASC) AS seq_no
    FROM valid
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM metrics a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 10)
),
metrics_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(promo_id, 2, 0) OVER (PARTITION BY promotion_promo_code ORDER BY event_seq_2 ASC, id ASC, promo_id ASC) AS next_promo_id,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_id DESC, id ASC, promo_id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC, promo_id ASC) AS seq_no_2
    FROM with_window
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('month', starts_on) AS starts_on_period
    FROM metrics_promotions
    WHERE promotion_promo_code IS NOT NULL
      AND promo_code IN ('BUNDLE', 'WELCOME5')
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    channel_filled,
    event_seq_2,
    promo_id,
    promotion_promo_code,
    next_promo_id
FROM cleaned
ORDER BY starts_on ASC, promotion_promo_code ASC;
