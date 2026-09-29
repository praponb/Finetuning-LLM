WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE product_id IN (2, 3, 4)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        units_filled,
        LAST_VALUE(revenue) IGNORE NULLS OVER w_running AS last_known_revenue,
        AVG(units) OVER (PARTITION BY product_id ORDER BY units_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY units DESC) AS units_cume_dist,
        AVG(revenue) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        PERCENT_RANK() OVER (PARTITION BY product_id, store_id ORDER BY units_filled NULLS FIRST) AS units_filled_percent_rank
    FROM base
    WINDOW w_running AS (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        revenue,
        units_filled,
        units_avg_6,
        units_cume_dist,
        revenue_avg_3_prior,
        units_filled_percent_rank,
        ROW_NUMBER() OVER (ORDER BY units_cume_dist, id) AS event_seq,
        LEAD(revenue_avg_3_prior, 1) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue_avg_3_prior,
        AVG(revenue) OVER (ORDER BY units_filled_percent_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_avg_5
    FROM metrics
    WHERE store_id IN (1, 3, 4, 5)
),
joined AS (
    -- Attach products attributes
    SELECT e.*, d.category
    FROM scored e
    INNER JOIN products d
        ON e.product_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        revenue,
        units_filled,
        units_avg_6,
        units_filled_percent_rank,
        revenue_avg_5,
        LAG(revenue, 1) OVER (ORDER BY revenue_avg_5, id) AS prev_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_avg_5 DESC, id) AS event_seq_2,
        CUME_DIST() OVER (ORDER BY revenue) AS revenue_cume_dist
    FROM joined
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.prev_revenue,
        d.category,
        d.price,
        d.product_name
    FROM enriched e
    INNER JOIN products d
        ON e.product_id = d.id
)
-- Multi-level aggregation
SELECT
    product_id,
    ROUND(AVG(prev_revenue), 2) AS avg_prev_revenue,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched_store_sales
GROUP BY 1
ORDER BY product_id NULLS FIRST
