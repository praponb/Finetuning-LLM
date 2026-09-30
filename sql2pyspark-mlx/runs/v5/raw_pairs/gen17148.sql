WITH kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM store_sales
    EXCEPT
    SELECT store_id, id
    FROM store_sales
    WHERE revenue > 3200
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(revenue, units) OVER w_part AS corr_revenue_units,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units_2
    FROM reconciled
    WHERE revenue BETWEEN 2100 AND 2200
    WINDOW w_part AS (PARTITION BY product_id, store_id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        units,
        revenue,
        corr_revenue_units_2,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id, product_id), 4) AS revenue_share_2,
        STDDEV_SAMP(corr_revenue_units_2) OVER w_ordered AS product_id_stddev_samp_corr_revenue_units_2,
        MIN(corr_revenue_units_2) OVER w_ordered AS product_id_min_corr_revenue_units_2
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY revenue) AS revenue_cume_dist,
        PERCENT_RANK() OVER (ORDER BY product_id_min_corr_revenue_units_2 NULLS FIRST) AS product_id_min_corr_revenue_units_2_percent_rank,
        DENSE_RANK() OVER (ORDER BY revenue_share_2 DESC NULLS LAST) AS revenue_share_2_dense_rank
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        revenue,
        revenue_share_2,
        product_id_min_corr_revenue_units_2,
        product_id_min_corr_revenue_units_2_percent_rank,
        CASE WHEN revenue_share_2 >= 0.67 THEN 'High' WHEN revenue_share_2 >= 0.38 THEN 'Medium' ELSE 'Low' END AS revenue_share_2_tier
    FROM scored
    WHERE revenue <= 2300
),
scored_store_sales AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (ORDER BY revenue DESC) AS revenue_cume_dist
    FROM cleaned
)
SELECT
    revenue,
    product_id_min_corr_revenue_units_2,
    product_id_min_corr_revenue_units_2_percent_rank,
    revenue_share_2_tier,
    revenue_cume_dist
FROM scored_store_sales
ORDER BY revenue DESC;
