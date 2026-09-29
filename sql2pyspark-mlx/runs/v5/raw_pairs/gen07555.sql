WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(starts_on, 'yyyy-MM-dd') AS starts_on_label
    FROM promotions
    WHERE ends_on BETWEEN '2024-04-01' AND '2025-09-04'
      AND channel IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_share
    FROM valid
    WHERE MONTH(ends_on) IN (2, 6, 8)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct_share,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        CUME_DIST() OVER (ORDER BY discount_pct_share DESC) AS discount_pct_share_cume_dist
    FROM with_windows
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on DESC) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct_share_cume_dist) OVER w_running AS running_avg_discount_pct_share_cume_dist,
        SUM(discount_pct_share_cume_dist) OVER w_running AS running_sum_discount_pct_share_cume_dist,
        MAX(discount_pct_share_cume_dist) OVER w_running AS running_max_discount_pct_share_cume_dist,
        COUNT(discount_pct_share_cume_dist) OVER w_running AS running_count_discount_pct_share_cume_dist
    FROM latest
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY discount_pct_share_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    promo_code,
    COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END) AS spring10_count,
    ROUND(AVG(discount_pct_share), 2) AS avg_discount_pct_share,
    COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows,
    MAX(running_sum_discount_pct_share_cume_dist) AS max_running_sum_discount_pct_share_cume_dist
FROM scored
GROUP BY 1
HAVING spring10_count >= 1
ORDER BY promo_code NULLS LAST;
