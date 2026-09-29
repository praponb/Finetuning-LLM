WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER w_recent AS running_sum_rating,
        MAX(rating) OVER w_recent AS running_max_rating,
        MIN(rating) OVER w_recent AS running_min_rating
    FROM suppliers
    WHERE supplier_name LIKE 'Pa%'
    WINDOW w_recent AS (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.running_max_rating,
        latest.product_launch_date,
        latest.product_cost
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_cost NULLS FIRST, id) AS rn
        FROM with_last
    ) t
    WHERE rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_cost DESC NULLS FIRST, id) AS rank_in_group
        FROM ranked
    ) x
    WHERE rank_in_group = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', product_launch_date) AS week_start,
    DATE_TRUNC('YEAR', product_launch_date) AS year_start,
    COUNT(DISTINCT country) AS distinct_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    PERCENTILE_APPROX(product_cost, 0.9) AS product_cost_p90,
    PERCENTILE_APPROX(running_max_rating, 0.9) AS running_max_rating_p90
FROM latest
GROUP BY DATE_TRUNC('WEEK', product_launch_date), DATE_TRUNC('YEAR', product_launch_date)
ORDER BY running_max_rating_p90
