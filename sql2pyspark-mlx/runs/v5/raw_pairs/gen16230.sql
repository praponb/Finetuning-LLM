WITH base AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN country IN ('UK', 'TH') THEN 'Tier 1' ELSE 'Other' END AS country_group,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 6
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost > 310
        )
),
not_flagged_suppliers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price >= 780
        )
),
cleaned AS (
    SELECT
        *,
        TRIM(supplier_name) AS clean_supplier_name,
        LENGTH(country) AS country_length
    FROM not_flagged_suppliers
    WHERE country = 'DE'
),
kept_keys AS (
    SELECT id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id
    FROM suppliers
    WHERE city = 'Berlin'
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    city,
    country,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    PERCENTILE_APPROX(country_length, ARRAY(0.25, 0.5, 0.75)) AS country_length_quartiles,
    APPROX_PERCENTILE(country_length, ARRAY(0.25, 0.5, 0.75)) AS country_length_quartiles_2,
    COUNT(DISTINCT city) AS distinct_cities
FROM reconciled
GROUP BY city, country
HAVING jp_rows > 1
ORDER BY distinct_cities DESC NULLS FIRST
