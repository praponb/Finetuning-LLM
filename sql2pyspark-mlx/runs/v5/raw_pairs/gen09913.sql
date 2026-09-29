WITH joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.revenue,
        p.category
    FROM store_sales e
    LEFT JOIN products p
        ON e.product_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        category,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY category), 4) AS revenue_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY category), 4) AS revenue_share_2,
        STDDEV_SAMP(revenue) OVER w_grp AS category_stddev_samp_revenue,
        ROW_NUMBER() OVER (ORDER BY revenue ASC, id ASC) AS event_seq
    FROM joined
    WINDOW w_grp AS (PARTITION BY category)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        revenue_share,
        revenue_share_2,
        category_stddev_samp_revenue,
        event_seq
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, category ORDER BY event_seq DESC, id ASC) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_seq,
        NTILE(10) OVER w_ordered AS revenue_share_2_bucket,
        AVG(category_stddev_samp_revenue) OVER (ORDER BY revenue_share_2 ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS category_stddev_samp_revenue_avg_3_prior
    FROM ranked
    WINDOW w_ordered AS (ORDER BY revenue_share_2 DESC, id ASC)
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(event_seq) OVER (ORDER BY revenue_share_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_seq,
        MIN(event_seq) OVER (ORDER BY revenue_share_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_event_seq,
        SUM(event_seq) OVER (ORDER BY revenue_share_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_event_seq,
        COUNT(event_seq) OVER (ORDER BY revenue_share_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_event_seq,
        CUME_DIST() OVER (ORDER BY event_seq DESC NULLS LAST) AS event_seq_cume_dist
    FROM enriched
    WHERE revenue_share_2_bucket <> 4
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_max_event_seq >= 6.8 THEN 'LARGE' ELSE 'SMALL' END AS running_max_event_seq_tier,
        ROUND(AVG(running_sum_event_seq), 2) AS avg_running_sum_event_seq,
        ROUND(AVG(revenue_share_2_bucket), 2) AS avg_revenue_share_2_bucket,
        MIN(running_max_event_seq) AS min_running_max_event_seq,
        APPROX_PERCENTILE(running_max_event_seq, 0.9) AS running_max_event_seq_p90,
        MAX(running_min_event_seq) AS max_running_min_event_seq,
        COUNT(DISTINCT id) AS distinct_ids
    FROM enriched_store_sales
    GROUP BY 1
)
SELECT
    running_max_event_seq_tier,
    avg_revenue_share_2_bucket,
    max_running_min_event_seq,
    distinct_ids
FROM grouped
ORDER BY avg_revenue_share_2_bucket DESC
