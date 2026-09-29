WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        LEAD(revenue, 1, 0) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS next_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share_2,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS LAST) AS revenue_percent_rank
    FROM store_sales
),
joined AS (
    -- Attach stores attributes
    SELECT e.*, d.store_name
    FROM with_windows e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
with_windows_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_share,
        next_revenue,
        revenue_share_2,
        revenue_percent_rank,
        store_name,
        AVG(next_revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_revenue_avg_5_prior,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY revenue_share DESC, id) AS revenue_share_bucket,
        RANK() OVER (ORDER BY revenue_percent_rank DESC NULLS LAST) AS revenue_percent_rank_rank,
        ROUND(next_revenue / SUM(next_revenue) OVER (PARTITION BY product_id), 4) AS next_revenue_share,
        AVG(revenue_percent_rank) OVER w_grp AS revenue_percent_rank_avg_6
    FROM joined
    WINDOW w_grp AS (ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows_store_sales
    QUALIFY RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue_share_2 DESC) = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN revenue_share >= 0.75 THEN 'LARGE' ELSE 'SMALL' END AS revenue_share_tier,
        store_id,
        ROUND(AVG(revenue_share), 2) AS avg_revenue_share,
        ROUND(AVG(revenue_percent_rank_avg_6), 2) AS avg_revenue_percent_rank_avg_6,
        MIN(revenue_share_bucket) AS min_revenue_share_bucket
    FROM deduped
    GROUP BY GROUPING SETS ((CASE WHEN revenue_share >= 0.75 THEN 'LARGE' ELSE 'SMALL' END, store_id), (CASE WHEN revenue_share >= 0.75 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    revenue_share_tier,
    store_id,
    avg_revenue_share,
    min_revenue_share_bucket
FROM grouped
ORDER BY min_revenue_share_bucket DESC;
