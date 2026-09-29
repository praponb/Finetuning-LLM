WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.category,
        x.product_stock_qty
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        city,
        category,
        product_stock_qty,
        LAG(rating) OVER (PARTITION BY category ORDER BY rating, id) AS prev_rating,
        LEAD(rating) OVER (PARTITION BY category ORDER BY rating, id) AS next_rating,
        LEAD(product_stock_qty) OVER (PARTITION BY category ORDER BY rating, id) AS next_product_stock_qty,
        LAG(product_stock_qty) OVER (PARTITION BY category ORDER BY rating, id) AS prev_product_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating, id) AS seq_no,
        AVG(rating) OVER (PARTITION BY city, category ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5
    FROM with_last
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        product_stock_qty,
        prev_rating,
        prev_product_stock_qty,
        rating_avg_5,
        FIRST_VALUE(prev_rating) IGNORE NULLS OVER (PARTITION BY city ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_rating,
        SUM(prev_rating) OVER (ORDER BY next_product_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_rating
    FROM metrics
    WHERE seq_no < 3
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        product_stock_qty,
        prev_rating,
        prev_product_stock_qty,
        rating_avg_5,
        first_known_prev_rating,
        UPPER(supplier_name) AS supplier_name_upper
    FROM with_windows
    WHERE UPPER(TRIM(supplier_name)) = 'SIAM PARTS CO'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.category IN ('Books', 'Fashion')
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_brand,
        top1.launch_date
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand, c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    product_stock_qty,
    prev_rating,
    prev_product_stock_qty,
    first_known_prev_rating,
    product_brand,
    CASE WHEN product_brand RLIKE '^(Umbrella|Globex)$' THEN 'OTHER_ZONE' WHEN product_brand RLIKE '^(Acme|Initech)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS product_brand_zone
FROM with_latest
ORDER BY prev_product_stock_qty;
