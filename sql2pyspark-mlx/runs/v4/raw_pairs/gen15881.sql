WITH with_last AS (
    SELECT s.*, x.store_sale_revenue
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    SELECT category, id
    FROM with_last
    INTERSECT ALL
    SELECT category, id
    FROM products
    WHERE category IN ('Fashion', 'Electronics')
),
reconciled AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.category = k.category AND s.id = k.id
),
with_dims AS (
    SELECT
        s.*,
        p2.supplier_name,
        p2.rating
    FROM reconciled s
    LEFT JOIN suppliers p2
        ON s.supplier_id = p2.id
),
valid AS (
    SELECT
        *,
        COALESCE(rating, 0) AS rating_filled,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM with_dims
    WHERE launch_date > DATE '2024-01-13'
      AND supplier_id IS NOT NULL
)
SELECT
    brand,
    price,
    cost,
    launch_date,
    supplier_id,
    supplier_name,
    rating,
    rating_filled,
    brand_filled
FROM valid
ORDER BY launch_date ASC NULLS LAST;
