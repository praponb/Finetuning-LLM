WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IN ('Tokyo', 'London', 'Singapore')
      AND city IS NOT DISTINCT FROM 'London'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.product_category,
        x.product_stock_qty
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.cost
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty BETWEEN 190 AND 272
        )
),
kept_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM kept
    INTERSECT
    SELECT city, id
    FROM suppliers
    WHERE city = 'New York'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN supplier_name_length >= 69 THEN 'LARGE' ELSE 'SMALL' END AS supplier_name_length_tier,
    CASE WHEN rating >= 4.5 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    ROUND(AVG(supplier_name_length), 2) AS avg_supplier_name_length,
    MAX(rating) FILTER (WHERE product_category = 'Books') AS books_max_rating,
    SORT_ARRAY(COLLECT_SET(product_category)) AS product_category_list,
    APPROX_PERCENTILE(product_stock_qty, 0.5) AS product_stock_qty_p50,
    SUM(rating) AS total_rating
FROM verified
GROUP BY 1, 2;
