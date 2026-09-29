WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        ROUND(rating * 0.85, 2) AS rating_scaled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Bangkok'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating_scaled NULLS FIRST) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
deduped_suppliers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rank_in_group) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Initech', 'Umbrella')
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_launch_date
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        AVG(rating_scaled) OVER w_ordered AS running_avg_rating_scaled,
        MIN(rating_scaled) OVER w_ordered AS running_min_rating_scaled,
        MAX(rating_scaled) OVER w_ordered AS running_max_rating_scaled
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_max_rating_scaled >= 82 THEN 'LARGE' ELSE 'SMALL' END AS running_max_rating_scaled_tier,
        APPROX_PERCENTILE(running_min_rating_scaled, ARRAY(0.25, 0.5, 0.75)) AS running_min_rating_scaled_quartiles,
        APPROX_PERCENTILE(running_avg_rating_scaled, ARRAY(0.25, 0.5, 0.75)) AS running_avg_rating_scaled_quartiles,
        ROUND(AVG(running_avg_rating_scaled), 2) AS avg_running_avg_rating_scaled,
        GROUPING(CASE WHEN running_max_rating_scaled >= 82 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
    FROM metrics
    GROUP BY ROLLUP (CASE WHEN running_max_rating_scaled >= 82 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING avg_running_avg_rating_scaled > 0
)
SELECT
    running_avg_rating_scaled_quartiles,
    avg_running_avg_rating_scaled,
    grouping_id
FROM stats;
