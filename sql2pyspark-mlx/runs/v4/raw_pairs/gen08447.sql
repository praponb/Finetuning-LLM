WITH filtered AS (
    SELECT *, CONCAT_WS(',', country, city) AS country_city_key
    FROM suppliers
    WHERE country = 'SG'
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost BETWEEN 390 AND 410
        )
),
cleaned AS (
    SELECT *, INITCAP(TRIM(supplier_name)) AS supplier_name_title
    FROM kept
    WHERE country IN ('UK', 'JP', 'TH')
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id ASC) AS rn
    FROM cleaned
    QUALIFY rn = 1
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    supplier_name_title,
    rn
FROM ranked
ORDER BY id DESC
LIMIT 20;
