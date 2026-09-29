WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(channel) AS channel_upper,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE starts_on >= DATE '2023-10-03'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(discount_pct) OVER w_hist AS next_discount_pct,
        LAG(discount_pct) OVER w_hist AS prev_discount_pct,
        LAG(discount_pct_int) OVER w_hist AS prev_discount_pct_int,
        ROW_NUMBER() OVER w_hist AS row_seq,
        SUM(discount_pct) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM valid
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct_int, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        prev_discount_pct,
        prev_discount_pct_int,
        row_seq,
        cumulative_discount_pct,
        NTILE(4) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        ROW_NUMBER() OVER (ORDER BY starts_on DESC, id) AS row_seq_2,
        NTILE(4) OVER w_hist AS row_seq_bucket,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM scored
    WINDOW w_hist AS (ORDER BY row_seq, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct_share DESC NULLS LAST) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_discount_pct_tier,
    AVG(row_seq_bucket) FILTER (WHERE channel = 'partner') AS partner_avg_row_seq_bucket,
    MAX(row_seq_2) AS max_row_seq_2,
    ROUND(AVG(discount_pct_bucket), 2) AS avg_discount_pct_bucket
FROM ranked
GROUP BY CASE WHEN cumulative_discount_pct >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY max_row_seq_2;
