WITH joined AS (
    -- Attach suppliers attributes
    SELECT e.*, d.rating
    FROM products e
    JOIN suppliers d
        ON e.supplier_id = d.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.unit_price < 150
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior,
        CORR(stock_qty, price) OVER (PARTITION BY category) AS corr_stock_qty_price,
        ROW_NUMBER() OVER (ORDER BY stock_qty DESC, id) AS seq_no
    FROM kept
),
enriched AS (
    -- Attach suppliers attributes
    SELECT e.*, s.rating AS supplier_rating
    FROM scored e
    JOIN suppliers s
        ON e.supplier_id = s.id
)
-- Grouped metrics
SELECT
    seq_no,
    CASE WHEN rating_avg_3_prior >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_avg_3_prior_tier,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count,
    SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price,
    COUNT(DISTINCT category) AS distinct_categories,
    APPROX_PERCENTILE(rating_avg_3_prior, 0.5) AS rating_avg_3_prior_p50,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    GROUPING(seq_no) AS gid
FROM enriched
GROUP BY CUBE (seq_no, CASE WHEN rating_avg_3_prior >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY rating_avg_3_prior_tier DESC
