WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '7' DAY
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(promotion_discount_pct) OVER (PARTITION BY promo_code) promo_code_count_promotion_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id, promo_id) row_seq,
        STDDEV_SAMP(promotion_discount_pct) OVER (PARTITION BY promo_code) promo_code_stddev_samp_promotion_discount_pct,
        NTILE(3) OVER (ORDER BY discount_pct, id, promo_id) discount_pct_bucket,
        SUM(promo_id) OVER (ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_promo_id
    FROM with_window
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        promo_id,
        promotion_discount_pct,
        cumulative_promo_id
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct DESC NULLS FIRST, id, promo_id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        promo_id,
        promotion_discount_pct,
        LEAD(cumulative_promo_id) OVER w_running next_cumulative_promo_id,
        LAG(promotion_discount_pct) OVER w_running prev_promotion_discount_pct,
        LEAD(promotion_discount_pct) OVER w_running next_promotion_discount_pct,
        ROW_NUMBER() OVER w_running seq_no,
        ROW_NUMBER() OVER w_hist row_seq
    FROM deduped
    WINDOW w_running AS (PARTITION BY promo_code, channel ORDER BY ends_on, id, promo_id), w_hist AS (PARTITION BY channel, promo_code ORDER BY promo_id DESC, id, promo_id)
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        ends_on,
        promo_id,
        promotion_discount_pct,
        next_promotion_discount_pct,
        seq_no,
        row_seq,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY next_promotion_discount_pct, id, promo_id) row_seq_2,
        SUM(row_seq) OVER (PARTITION BY promo_code, channel) promo_code_sum_row_seq,
        FIRST_VALUE(row_seq, TRUE) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_row_seq
    FROM enriched
    WHERE promo_id <= 8
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows_promotions src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN discount_pct >= 0.35 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
        promo_id,
        APPROX_PERCENTILE(first_known_row_seq, 0.95) AS first_known_row_seq_p95,
        AVG(row_seq_2) AS avg_row_seq_2,
        COUNT(*) AS cnt,
        COUNT(*) AS cnt_2,
        COUNT(DISTINCT promo_code) AS distinct_promo_codes,
        GROUPING_ID() AS gid
    FROM with_window_promotions
    GROUP BY ROLLUP (CASE WHEN discount_pct >= 0.35 THEN 'HIGH' ELSE 'LOW' END, promo_id)
)
SELECT
    discount_pct_tier,
    first_known_row_seq_p95,
    cnt,
    cnt_2,
    distinct_promo_codes,
    gid
FROM totals
ORDER BY distinct_promo_codes DESC, gid
