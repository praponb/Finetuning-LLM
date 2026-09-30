WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CONCAT(category, ' ', product_name) AS category_product_name,
        COALESCE(stock_qty, 0) AS stock_qty_filled
    FROM products
    WHERE launch_date > TO_DATE('2023-03-18')
      AND brand IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT supplier_id, brand
    FROM filtered
    EXCEPT ALL
    SELECT supplier_id, brand
    FROM products
    WHERE launch_date > '2024-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.brand = k.brand
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.store_id IN (1, 2, 4, 6)
        )
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.price,
        src.cost,
        src.stock_qty,
        src.tags,
        src.supplier_id,
        b.country,
        b.supplier_name
    FROM eligible src
    LEFT JOIN suppliers b
        ON src.supplier_id = b.id
    WHERE b.country NOT IN ('UK', 'US', 'JP')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        supplier_id,
        country,
        DENSE_RANK() OVER (PARTITION BY brand, country ORDER BY cost DESC) AS cost_dense_rank,
        MAX(stock_qty) OVER (PARTITION BY country, supplier_id) AS country_max_stock_qty
    FROM joined
)
-- Aggregate with conditional metrics
SELECT
    supplier_id,
    ROUND(AVG(cost), 2) AS avg_cost,
    CORR(stock_qty, cost) AS corr_stock_qty_cost,
    STDDEV_SAMP(cost) AS cost_stddev
FROM enriched
GROUP BY 1
