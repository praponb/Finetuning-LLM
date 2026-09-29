WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(channel) AS clean_channel
    FROM promotions
    WHERE starts_on > DATE '2023-10-10'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(discount_pct, 2) OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id) AS next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        clean_channel,
        next_discount_pct,
        seq_no,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        NTILE(5) OVER (PARTITION BY channel, promo_code ORDER BY next_discount_pct, id) AS next_discount_pct_bucket,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id) AS seq_no_2,
        SUM(next_discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY next_discount_pct, id) AS seq_no_3
    FROM metrics
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 10)
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    clean_channel,
    next_discount_pct,
    seq_no,
    discount_pct_share
FROM matched
ORDER BY channel DESC NULLS FIRST
