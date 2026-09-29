WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ARRAY_JOIN(tags, ', ') tags_text,
        CASE WHEN brand IN ('Stark', 'Globex') THEN 'Group A' ELSE 'Other' END brand_group
    FROM products
    WHERE stock_qty = 308
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.category,
        src.brand,
        src.supplier_id,
        p.country,
        p.city,
        p.rating
    FROM valid src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS LAST, id) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date <= '2025-07-01'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.quantity >= 7
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.supplier_id,
        e.country,
        e.city,
        b.rating AS supplier_rating,
        b.city AS supplier_city
    FROM kept e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
)
-- Grouped metrics
SELECT
    country,
    ROUND(AVG(supplier_rating), 2) AS avg_supplier_rating,
    SUM(CASE WHEN country = 'US' THEN supplier_rating ELSE 0 END) AS us_supplier_rating,
    COUNT_IF(city = 'London') AS london_events
FROM enriched
GROUP BY country;
