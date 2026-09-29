WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.price,
        src.cost,
        src.stock_qty,
        src.supplier_id,
        s.city,
        s.rating
    FROM products src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (10, 30)
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT supplier_id, id
    FROM not_flagged
    EXCEPT ALL
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id IN (8, 15, 36)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM kept_rows
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM scored
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE category IN ('Books', 'Electronics')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys_2 k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(rating_share) OVER (PARTITION BY supplier_id) AS supplier_id_stddev_samp_rating_share,
        STDDEV_SAMP(rating) OVER (PARTITION BY supplier_id) AS supplier_id_stddev_samp_rating
    FROM reconciled
),
remaining_keys_3 AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE launch_date BETWEEN '2023-06-01' AND '2023-08-26'
),
kept_rows_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys_3 k ON s.id = k.id
)
-- Grouped metrics
SELECT
    city,
    ROUND(AVG(supplier_id_stddev_samp_rating), 2) AS avg_supplier_id_stddev_samp_rating,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS stock_qty_per_new_york,
    MIN(price) AS min_price
FROM kept_rows_products
GROUP BY city;
