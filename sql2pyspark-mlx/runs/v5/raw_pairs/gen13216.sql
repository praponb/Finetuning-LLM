WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        MAX(sqft) OVER w_ordered AS last7_max_sqft,
        AVG(sqft) OVER w_ordered AS last7_avg_sqft,
        MIN(sqft) OVER w_ordered AS last7_min_sqft,
        COUNT(sqft) OVER w_ordered AS last7_count_sqft,
        LAG(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft
    FROM stores
    WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_units,
        top1.store_sale_sale_date
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.store_id = 20
        )
),
enriched_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        last7_max_sqft,
        last7_avg_sqft,
        last7_count_sqft,
        SUM(last7_max_sqft) OVER (ORDER BY store_sale_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_max_sqft,
        LAST_VALUE(store_sale_units, TRUE) OVER (PARTITION BY region ORDER BY last7_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_store_sale_units,
        LEAD(last7_max_sqft, 1) OVER (PARTITION BY region ORDER BY last7_count_sqft, id) AS next_last7_max_sqft,
        ROUND(last7_avg_sqft / SUM(last7_avg_sqft) OVER (PARTITION BY region), 4) AS last7_avg_sqft_share,
        LAST_VALUE(last7_avg_sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY store_sale_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_avg_sqft
    FROM kept
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY next_last7_max_sqft DESC NULLS FIRST, id) AS dedup_rank
        FROM enriched_stores
    ) t
    WHERE dedup_rank = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last7_avg_sqft_share >= 0.69 THEN 'HIGH' ELSE 'LOW' END AS last7_avg_sqft_share_tier,
        region,
        COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        AVG(last7_avg_sqft) AS avg_last7_avg_sqft,
        COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
        APPROX_PERCENTILE(next_last7_max_sqft, 0.5) AS next_last7_max_sqft_p50,
        MIN(last7_count_sqft) AS min_last7_count_sqft
    FROM ranked
    GROUP BY 1, 2
)
SELECT
    last7_avg_sqft_share_tier,
    region,
    region_list,
    avg_last7_avg_sqft,
    east_count
FROM stats
ORDER BY region NULLS LAST;
