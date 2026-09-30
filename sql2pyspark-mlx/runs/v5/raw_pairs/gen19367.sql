WITH kept AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date >= s.launch_date
        )
),
latest AS (
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY category, brand ORDER BY cost) = 1
),
enriched AS (
    SELECT
        e.id,
        e.brand,
        e.cost,
        e.launch_date,
        e.tags,
        e.supplier_id,
        b.country
    FROM latest e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.country IN ('TH', 'JP')
),
kept_keys AS (
    SELECT id, supplier_id
    FROM enriched
    EXCEPT ALL
    SELECT id, supplier_id
    FROM products
    WHERE UPPER(product_name) LIKE '%OGA%'
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
cleaned AS (
    SELECT
        *,
        LOWER(brand) AS brand_lower,
        COALESCE(country, 'NONE') AS resolved_country
    FROM reconciled
    WHERE brand = 'Stark'
),
kept_products AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.unit_price < 120
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (6, 25)
        )
)
SELECT
    p.city,
    s.country AS product_country,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.cost), 2) AS total_cost
FROM not_flagged s
LEFT JOIN suppliers p ON s.supplier_id = p.id
GROUP BY p.city, s.country;
