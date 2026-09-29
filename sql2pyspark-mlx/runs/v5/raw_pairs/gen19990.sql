WITH base AS (
    SELECT
        *,
        SUBSTRING(channel, 1, 4) AS channel_prefix,
        LENGTH(channel) AS channel_length,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE YEAR(ends_on) >= 2024
),
with_windows AS (
    SELECT
        *,
        SUM(channel_length) OVER (ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_channel_length,
        SUM(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_sum_discount_pct,
        AVG(channel_length) OVER w_hist AS channel_length_avg_2
    FROM base
    WINDOW w_hist AS (PARTITION BY promo_code ORDER BY channel_length ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY discount_pct ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id ASC) AS row_seq_2,
        ROW_NUMBER() OVER (ORDER BY starts_on DESC, id ASC) AS seq_no
    FROM with_windows
),
attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 10)
),
ranked AS (
    SELECT *
    FROM attributed
    QUALIFY RANK() OVER (PARTITION BY promotion_promo_code ORDER BY promo_code_sum_discount_pct DESC) = 1
),
ranked_promotions AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY seq_no ASC NULLS LAST, id ASC, promo_id ASC) = 1
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    running_channel_length,
    promo_code_sum_discount_pct,
    row_seq_2,
    seq_no,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    SUM(running_channel_length) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_channel_length,
    MAX(running_channel_length) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_channel_length,
    MIN(running_channel_length) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_channel_length,
    ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id ASC, promo_id ASC) AS seq_no_2
FROM ranked_promotions
WHERE promo_id != 9;
