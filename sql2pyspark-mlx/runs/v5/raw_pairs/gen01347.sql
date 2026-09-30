WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        tags,
        supplier_id,
        ADD_MONTHS(launch_date, 1) AS launch_date_plus_months,
        CASE WHEN category = 'Books' THEN 1 ELSE 0 END AS is_books
    FROM products
    WHERE cost BETWEEN 490 AND 520
),
with_latest AS (
    SELECT
        s.*,
        top1.last_restock,
        top1.inventory_quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid_products AS (
    SELECT
        *,
        price - inventory_quantity AS price_minus_inventory_quantity,
        DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label
    FROM with_latest
    WHERE brand IS DISTINCT FROM 'Globex'
      AND launch_date >= TO_DATE('2023-06-15')
),
enriched AS (
    SELECT e.*, d.rating
    FROM valid_products e
    JOIN suppliers d
        ON e.supplier_id = d.id
),
with_latest_products AS (
    SELECT s.*, top1.inventory_last_restock
    FROM enriched s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.last_restock
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    SELECT
        e.id,
        e.product_name,
        e.price,
        e.launch_date,
        e.supplier_id,
        e.launch_date_plus_months,
        e.last_restock,
        e.price_minus_inventory_quantity,
        e.launch_date_label,
        e.rating,
        b.city,
        b.rating AS supplier_rating
    FROM with_latest_products e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.country = 'UK'
),
enriched_products AS (
    SELECT
        a.id,
        a.price,
        a.launch_date,
        a.supplier_id,
        a.last_restock,
        a.rating,
        a.city,
        s.country,
        s.rating AS supplier_rating_2
    FROM joined a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
)
SELECT
    d.country AS supplier_country,
    d.city AS supplier_city,
    s.city AS product_city,
    COUNT(*) AS num_rows,
    MIN(s.supplier_rating_2) AS min_supplier_rating_2,
    GROUPING_ID() AS level_id
FROM enriched_products s
JOIN suppliers d ON s.supplier_id = d.id
GROUP BY GROUPING SETS ((d.country, d.city, s.city), (d.country, d.city), (d.country), ())
