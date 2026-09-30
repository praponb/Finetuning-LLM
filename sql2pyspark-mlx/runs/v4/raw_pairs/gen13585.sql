WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE starts_on >= DATE '2023-04-25'
      AND channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS seq_no,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY ends_on, id) AS next_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS row_seq,
        NTILE(3) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket
    FROM base
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows src
    JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 3)
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    channel_lower,
    resolved_channel,
    seq_no,
    next_discount_pct
FROM with_window;
