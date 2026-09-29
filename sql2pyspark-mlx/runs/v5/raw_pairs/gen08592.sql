WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name IS NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.category,
        latest.product_brand
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.category, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_rating,
        AVG(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_rating,
        SUM(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_rating,
        MIN(rating) OVER (PARTITION BY category ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_rating,
        ROW_NUMBER() OVER (PARTITION BY city, product_brand ORDER BY rating DESC, id) AS event_seq
    FROM with_latest
    WHERE product_brand = 'Initech'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name_slug,
        country_supplier_name_key,
        category,
        last3_count_rating,
        last3_avg_rating,
        last3_min_rating,
        ROW_NUMBER() OVER (ORDER BY last3_avg_rating DESC, id) AS event_seq_2,
        LAG(last3_avg_rating, 1) OVER (PARTITION BY product_brand, category ORDER BY last3_sum_rating, id) AS prev_last3_avg_rating,
        ROUND(last3_avg_rating / SUM(last3_avg_rating) OVER (PARTITION BY country), 4) AS last3_avg_rating_share
    FROM with_windows
),
with_latest_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_cost,
        latest.product_launch_date
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN product_cost >= 670 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_cost_tier,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    ROUND(SUM(last3_count_rating) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS last3_count_rating_per_grocery
FROM with_latest_suppliers
GROUP BY 1
HAVING COUNT_IF(category = 'Fashion') >= 1
ORDER BY last3_count_rating_per_grocery DESC
