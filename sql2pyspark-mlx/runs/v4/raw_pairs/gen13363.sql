WITH kept_keys AS (
    SELECT category
    FROM products
    EXCEPT ALL
    SELECT category
    FROM products
    WHERE price IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM products s
    INNER JOIN kept_keys k ON s.category = k.category
),
cleaned AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
    FROM kept_rows
    WHERE launch_date > TO_DATE('2023-04-25')
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock BETWEEN '2023-10-16' AND '2025-03-25'
        )
),
joined AS (
    SELECT s.*, b.city
    FROM kept s
    INNER JOIN suppliers b
        ON s.supplier_id = b.id
),
joined_products AS (
    SELECT
        src.*,
        b.rating,
        b.supplier_name
    FROM joined src
    LEFT JOIN suppliers b
        ON src.supplier_id = b.id
),
enriched AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        days_since_launch_date,
        city,
        rating,
        supplier_name,
        CUME_DIST() OVER (PARTITION BY category ORDER BY rating ASC NULLS FIRST) AS rating_cume_dist,
        AVG(days_since_launch_date) OVER (PARTITION BY supplier_id) AS supplier_id_avg_days_since_launch_date,
        RANK() OVER (PARTITION BY city ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY city), 4) AS cost_share
    FROM joined_products
),
joined_products_2 AS (
    SELECT
        s.*,
        d.supplier_name AS supplier_supplier_name,
        d.city AS supplier_city,
        d.rating AS supplier_rating
    FROM enriched s
    INNER JOIN suppliers d
        ON s.supplier_id = d.id
),
stats AS (
    SELECT
        DATE_TRUNC('YEAR', launch_date) AS year_start,
        SUM(days_since_launch_date) AS total_days_since_launch_date,
        COUNT(DISTINCT city) AS distinct_cities,
        COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count,
        SUM(supplier_id_avg_days_since_launch_date) FILTER (WHERE supplier_city = 'Berlin') AS berlin_total_supplier_id_avg_days_since_launch_date
    FROM joined_products_2
    GROUP BY 1
)
SELECT
    year_start,
    total_days_since_launch_date,
    distinct_cities,
    berlin_total_supplier_id_avg_days_since_launch_date
FROM stats;
