WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(stock_qty, 0) stock_qty_filled,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE launch_date > DATE '2023-05-02'
      AND brand IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'clearance')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price BETWEEN 170 AND 230
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY brand ORDER BY price ASC, id ASC) price_bucket,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY price DESC, id ASC) price_bucket_2,
        NTILE(4) OVER (PARTITION BY supplier_id ORDER BY price ASC, id ASC) price_bucket_3
    FROM not_flagged
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT s.*, d.country
    FROM metrics s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        p.city,
        p.country AS supplier_country,
        p.supplier_name
    FROM with_dims e
    INNER JOIN suppliers p
        ON e.supplier_id = p.id
),
metrics_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY supplier_country, city ORDER BY price_bucket_3 ASC, id ASC) price_bucket_3_bucket,
        NTILE(3) OVER w_recent price_bucket_bucket,
        SUM(stock_qty) OVER (PARTITION BY country) country_sum_stock_qty
    FROM enriched
    WINDOW w_recent AS (PARTITION BY brand, category ORDER BY price_bucket DESC, id ASC)
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        d.city AS supplier_city,
        d.country AS supplier_country_2
    FROM metrics_products a
    INNER JOIN suppliers d
        ON a.supplier_id = d.id
)
-- Window metrics over several different partitions
SELECT
    *,
    NTILE(4) OVER w_recent price_bucket_2_bucket,
    LEAD(stock_qty, 1) OVER w_ordered next_stock_qty,
    LEAD(price_bucket_2, 2) OVER w_ordered next_price_bucket_2,
    ROUND(price_bucket / SUM(price_bucket) OVER (PARTITION BY supplier_country_2), 4) price_bucket_share
FROM with_dims_products
WINDOW w_recent AS (PARTITION BY supplier_country_2 ORDER BY price_bucket_2 DESC, id ASC), w_ordered AS (ORDER BY price_bucket_2 ASC, id ASC)
