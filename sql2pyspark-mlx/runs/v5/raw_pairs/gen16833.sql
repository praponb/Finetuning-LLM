WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(channel)) channel_title,
        ADD_MONTHS(ends_on, 3) ends_on_plus_months,
        COALESCE(channel, 'N/A') channel_clean
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) discount_pct_bucket,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) discount_pct_avg_5_prior,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        channel_title,
        ends_on_plus_months,
        discount_pct_avg_5_prior,
        LAG(discount_pct_bucket) OVER w_part prev_discount_pct_bucket,
        LAG(discount_pct) OVER w_part prev_discount_pct,
        LEAD(discount_pct) OVER w_part next_discount_pct,
        ROW_NUMBER() OVER w_part seq_no,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_avg_5_prior, id) row_seq
    FROM scored
    WHERE discount_pct BETWEEN 0.19 AND 0.24
    WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct_bucket, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(prev_discount_pct) OVER w_grp running_max_prev_discount_pct,
        SUM(prev_discount_pct) OVER w_grp running_sum_prev_discount_pct,
        COUNT(prev_discount_pct) OVER w_grp running_count_prev_discount_pct,
        MIN(prev_discount_pct) OVER w_grp running_min_prev_discount_pct
    FROM enriched
    WINDOW w_grp AS (PARTITION BY channel ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    channel_title,
    ends_on_plus_months,
    discount_pct_avg_5_prior,
    prev_discount_pct_bucket,
    prev_discount_pct
FROM metrics
