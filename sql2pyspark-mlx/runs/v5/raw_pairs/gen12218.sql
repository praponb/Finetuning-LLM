WITH scored AS (
    SELECT
        id,
        sqft,
        COUNT(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        MAX(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft
    FROM stores
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY running_max_sqft, id) AS row_seq,
        CUME_DIST() OVER w_grp AS running_sum_sqft_cume_dist
    FROM scored
    WINDOW w_grp AS (ORDER BY running_sum_sqft DESC NULLS FIRST)
),
metrics AS (
    SELECT
        id,
        running_count_sqft,
        running_max_sqft,
        row_seq,
        running_sum_sqft_cume_dist,
        MAX(running_avg_sqft) OVER w_part AS running_max_running_avg_sqft,
        SUM(running_avg_sqft) OVER w_part AS running_sum_running_avg_sqft,
        AVG(running_avg_sqft) OVER w_part AS running_avg_running_avg_sqft,
        FIRST_VALUE(running_sum_sqft_cume_dist) IGNORE NULLS OVER w_recent AS first_known_running_sum_sqft_cume_dist
    FROM enriched
    WHERE running_sum_sqft_cume_dist < 0.57
    WINDOW w_part AS (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY running_avg_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue IS NOT NULL
        )
)
SELECT
    id,
    running_count_sqft,
    row_seq,
    running_sum_sqft_cume_dist,
    running_max_running_avg_sqft,
    running_sum_running_avg_sqft,
    running_avg_running_avg_sqft
FROM eligible
ORDER BY running_avg_running_avg_sqft DESC NULLS LAST, running_sum_sqft_cume_dist DESC;
