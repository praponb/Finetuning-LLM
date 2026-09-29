WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        SUBSTRING(channel, 1, 3) AS channel_prefix,
        CEIL(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2023-05-20'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
filtered_promotions AS (
    SELECT
        *,
        DATE_ADD(ends_on, 4) AS ends_on_plus_4d,
        COALESCE(channel, 'UNKNOWN') AS channel_filled_2
    FROM latest
    WHERE starts_on > DATE '2023-06-28'
      AND channel IS NOT NULL
      AND discount_pct <= 0.22
),
attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered_promotions e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '14' DAY
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY discount_pct_whole, id, promo_id) AS discount_pct_whole_bucket,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_whole DESC, id, promo_id) AS seq_no
    FROM attributed
),
deduped AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY latest_rank, id, promo_id) = 1
),
matched_keys AS (
    SELECT channel, id
    FROM deduped
    EXCEPT DISTINCT
    SELECT channel, id
    FROM promotions
    WHERE promo_code NOT IN ('WELCOME5', 'BUNDLE', 'SPRING10')
),
verified AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.channel = k.channel AND s.id = k.id
)
SELECT
    channel,
    starts_on,
    ends_on,
    discount_pct_whole,
    channel_filled,
    ends_on_plus_4d,
    channel_filled_2,
    promo_id,
    promotion_promo_code
FROM verified
ORDER BY promotion_promo_code DESC NULLS LAST
