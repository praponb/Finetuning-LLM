WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN country IN ('SG', 'UK') THEN 'Priority' ELSE 'Other' END AS country_group,
        FLOOR(rating) AS rating_whole
    FROM suppliers
    WHERE city IS DISTINCT FROM 'London'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date <= '2025-06-01'
        )
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IN (12, 19)
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(rating_whole, rating) AS least_rating_whole_rating,
        TRIM(supplier_name) AS clean_supplier_name,
        COALESCE(city, 'NONE') AS resolved_city
    FROM eligible_suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY least_rating_whole_rating DESC) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND UPPER(x.product_name) LIKE '%SB-%'
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        city,
        CASE WHEN rating >= 3.5 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
        MIN(least_rating_whole_rating) AS min_least_rating_whole_rating,
        MAX(rating_whole) FILTER (WHERE city = 'Bangkok') AS bangkok_max_rating_whole,
        COUNT_IF(city = 'Bangkok') AS bangkok_events
    FROM not_flagged
    GROUP BY city, rating_tier
)
SELECT
    city,
    rating_tier,
    bangkok_events
FROM stats
ORDER BY bangkok_events DESC, rating_tier DESC;
