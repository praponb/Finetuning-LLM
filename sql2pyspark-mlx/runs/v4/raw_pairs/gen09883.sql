WITH enriched AS (
    -- Attach stores attributes
    SELECT s.*, b.city
    FROM store_sales s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(units, 2) units_rounded,
        COALESCE(city, 'NONE') resolved_city
    FROM enriched
    WHERE sale_date >= DATE '2023-06-14'
      AND store_id IN (1, 4, 5)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        city,
        units_rounded,
        resolved_city,
        AVG(units_rounded) OVER (PARTITION BY city ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) units_rounded_avg_5_prior,
        LAST_VALUE(revenue, TRUE) OVER w_ordered last_known_revenue,
        LAG(units_rounded, 1) OVER w_recent prev_units_rounded
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY product_id ORDER BY sale_date, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY units DESC NULLS LAST, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM ranked
    INTERSECT ALL
    SELECT product_id
    FROM store_sales
    WHERE sale_date < '2024-06-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY units_rounded_avg_5_prior DESC NULLS FIRST) units_rounded_avg_5_prior_dense_rank,
        RANK() OVER (ORDER BY prev_units_rounded DESC NULLS LAST) prev_units_rounded_rank,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units_rounded DESC NULLS LAST) units_rounded_dense_rank,
        MIN(last_known_revenue) OVER (PARTITION BY store_id, product_id) store_id_min_last_known_revenue
    FROM verified
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY units_rounded_dense_rank NULLS FIRST) units_rounded_dense_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY store_id_min_last_known_revenue DESC) store_id_min_last_known_revenue_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units) units_percent_rank,
        ROUND(prev_units_rounded_rank / SUM(prev_units_rounded_rank) OVER (PARTITION BY city), 4) prev_units_rounded_rank_share
    FROM enriched_store_sales
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN units_rounded_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS units_rounded_dense_rank_tier,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    SUM(store_id_min_last_known_revenue_cume_dist) FILTER (WHERE city = 'London') AS london_total_store_id_min_last_known_revenue_cume_dist,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows
FROM metrics
GROUP BY CASE WHEN units_rounded_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY singapore_rows DESC NULLS LAST
