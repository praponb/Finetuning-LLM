WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        LAST_DAY(ends_on) AS ends_on_month_end,
        LENGTH(promo_code) AS promo_code_length
    FROM promotions
    WHERE ends_on > TO_DATE('2023-07-01')
      AND channel IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        ends_on_month_end,
        promo_code_length,
        SUBSTRING(channel, 1, 4) AS channel_prefix,
        CEIL(discount_pct) AS discount_pct_whole
    FROM base
    WHERE channel IS NOT NULL
      AND discount_pct BETWEEN 0.2 AND 0.23
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        ends_on_month_end,
        promo_code_length,
        channel_prefix,
        discount_pct_whole,
        LAST_DAY(ends_on) AS ends_on_month_end_2
    FROM filtered
    WHERE promo_code_length = 56
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        ends_on_month_end,
        promo_code_length,
        channel_prefix,
        discount_pct_whole,
        SUM(discount_pct_whole) OVER w_ordered AS running_discount_pct_whole,
        NTILE(4) OVER (ORDER BY promo_code_length, id) AS promo_code_length_bucket
    FROM cleaned
    WHERE ends_on_month_end BETWEEN '2023-06-01' AND '2025-09-01'
    WINDOW w_ordered AS (ORDER BY ends_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        ends_on_month_end,
        promo_code_length,
        discount_pct_whole,
        running_discount_pct_whole,
        promo_code_length_bucket,
        SUM(promo_code_length_bucket) OVER w_running AS cumulative_promo_code_length_bucket,
        LEAD(discount_pct_whole, 1) OVER (ORDER BY discount_pct_whole, id) AS next_discount_pct_whole
    FROM scored
    WINDOW w_running AS (ORDER BY ends_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        ends_on_month_end,
        discount_pct_whole,
        promo_code_length_bucket,
        NTILE(4) OVER (ORDER BY discount_pct_whole, id) AS discount_pct_whole_bucket,
        SUM(cumulative_promo_code_length_bucket) OVER (ORDER BY running_discount_pct_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_promo_code_length_bucket,
        SUM(discount_pct_whole) OVER (ORDER BY ends_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_whole_2,
        LAG(promo_code_length, 2) OVER (ORDER BY running_discount_pct_whole, id) AS prev_promo_code_length,
        AVG(discount_pct_whole) OVER (ORDER BY promo_code_length_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_whole_avg_5_prior
    FROM enriched
)
SELECT
    id,
    ends_on_month_end,
    discount_pct_whole,
    discount_pct_whole_bucket,
    cumulative_cumulative_promo_code_length_bucket,
    running_discount_pct_whole_2,
    prev_promo_code_length
FROM enriched_promotions
ORDER BY discount_pct_whole_bucket, ends_on_month_end
