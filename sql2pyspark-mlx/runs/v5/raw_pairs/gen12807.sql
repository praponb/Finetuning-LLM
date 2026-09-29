WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (12, 18, 23)
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city
    FROM eligible
    EXCEPT
    SELECT city
    FROM suppliers
    WHERE UPPER(supplier_name) LIKE '%IAM%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.city = k.city
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name IS NOT NULL
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'NONE') AS resolved_city
    FROM kept
    WHERE city IS NOT DISTINCT FROM 'Tokyo'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        resolved_city,
        COUNT(rating) OVER (PARTITION BY country) AS country_count_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share
    FROM filtered
)
-- Grouped metrics
SELECT
    city,
    CASE WHEN country_count_rating >= 18 THEN 'HIGH' ELSE 'LOW' END AS country_count_rating_tier,
    SUM(rating_share) AS total_rating_share,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'New York' THEN country_count_rating ELSE 0 END) AS new_york_country_count_rating,
    COUNT(*) AS row_count
FROM metrics
GROUP BY city, CASE WHEN country_count_rating >= 18 THEN 'HIGH' ELSE 'LOW' END
HAVING num_cities < 1000;
