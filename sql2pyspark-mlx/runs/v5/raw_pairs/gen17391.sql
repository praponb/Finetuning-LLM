WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(country) AS country_upper,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        city
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country, city ORDER BY rating ASC NULLS FIRST) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.category,
        top1.product_brand
    FROM ranked s
    JOIN LATERAL (
            SELECT c.category, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Electronics'
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.launch_date
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        city,
        LAST_VALUE(city, TRUE) OVER w_ordered AS last_known_city,
        LAST_VALUE(product_brand) IGNORE NULLS OVER w_ordered AS last_known_product_brand,
        FIRST_VALUE(city, TRUE) OVER w_grp AS first_known_city
    FROM with_last
    WHERE product_brand IN ('Initech', 'Acme', 'Umbrella')
    WINDOW w_ordered AS (PARTITION BY category ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY city ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    city,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows
FROM scored
GROUP BY city
HAVING COUNT_IF(city = 'Bangkok') < 1000
ORDER BY london_count DESC;
