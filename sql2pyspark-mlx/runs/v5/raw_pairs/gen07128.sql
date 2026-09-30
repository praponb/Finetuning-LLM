WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS revenue_bucket,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        STDDEV_SAMP(revenue) OVER w_part AS store_id_stddev_samp_revenue,
        AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_avg_2
    FROM store_sales
    WINDOW w_part AS (PARTITION BY store_id)
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.revenue,
        src.cumulative_units,
        b.stock_qty
    FROM enriched src
    INNER JOIN products b
        ON src.product_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue DESC NULLS FIRST, id) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        revenue,
        stock_qty,
        rn,
        NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id) AS revenue_bucket,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_cume_dist,
        ROUND(cumulative_units / SUM(cumulative_units) OVER (PARTITION BY store_id), 4) AS cumulative_units_share
    FROM deduped
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN revenue >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    CASE WHEN revenue_cume_dist >= 0.78 THEN 'LARGE' ELSE 'SMALL' END AS revenue_cume_dist_tier,
    MIN(cumulative_units_share) AS min_cumulative_units_share,
    COUNT(DISTINCT id) AS distinct_ids,
    RANK() OVER (PARTITION BY CASE WHEN revenue >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_rank
FROM with_windows
GROUP BY revenue_tier, revenue_cume_dist_tier;
