WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.supplier_name
    FROM products a
    JOIN suppliers s
        ON a.supplier_id <=> s.id
    WHERE s.supplier_name LIKE '%Trade%'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        rating,
        supplier_name,
        MAX(rating) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        MIN(rating) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_rating,
        AVG(rating) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rating,
        SUM(rating) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating,
        SUM(rating) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM with_dims
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.store_id IN (4, 5)
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.stock_qty,
        s.supplier_id,
        s.rating,
        s.running_max_rating,
        s.running_avg_rating,
        s.cumulative_rating,
        p2.city,
        p2.supplier_name AS supplier_supplier_name,
        p2.rating AS supplier_rating
    FROM not_flagged s
    JOIN suppliers p2
        ON s.supplier_id = p2.id
),
with_windows_products AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        stock_qty,
        city,
        supplier_supplier_name,
        supplier_rating,
        SUM(cumulative_rating) OVER (PARTITION BY supplier_id ORDER BY cumulative_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_rating,
        CUME_DIST() OVER (PARTITION BY category, city ORDER BY running_max_rating DESC NULLS FIRST) AS running_max_rating_cume_dist,
        LEAD(running_avg_rating, 2) OVER (PARTITION BY category, supplier_id ORDER BY stock_qty, id) AS next_running_avg_rating,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY rating DESC) AS rating_percent_rank
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN supplier_rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END AS supplier_rating_tier,
    city,
    CASE WHEN stock_qty >= 378 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
    MAX(next_running_avg_rating) AS max_next_running_avg_rating,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    SUM(CASE WHEN category = 'Home' THEN rating_percent_rank ELSE 0 END) AS home_rating_percent_rank,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM with_windows_products
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN category = 'Home' THEN rating_percent_rank ELSE 0 END) > 0;
