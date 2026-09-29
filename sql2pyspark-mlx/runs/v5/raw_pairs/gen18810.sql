WITH base AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN rating >= 3.5 THEN 'High' WHEN rating >= 2.0 THEN 'Medium' ELSE 'Low' END AS rating_level,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE rating <= 3.2
),
with_last AS (
    SELECT s.*, x.category
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost <= 400
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category IN ('Sports', 'Electronics', 'Grocery')
        )
),
eligible_suppliers AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price BETWEEN 1060 AND 1150
        )
)
SELECT
    category,
    COUNT_IF(category = 'Home') AS home_events,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count,
    COUNT(*) AS cnt,
    GROUPING(category) AS gid
FROM eligible_suppliers
GROUP BY ROLLUP (category);
