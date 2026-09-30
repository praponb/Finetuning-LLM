WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(country) AS clean_country
    FROM suppliers
    WHERE city IS NOT NULL
      AND country = 'DE'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', country, city) AS country_city_key,
        ROUND(rating * 1.07, 2) AS rating_scaled,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM valid
    WHERE city IS NOT NULL
),
cleaned_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        clean_country,
        country_city_key,
        rating_scaled,
        city_filled,
        UPPER(supplier_name) AS supplier_name_upper
    FROM cleaned
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
),
cleaned_suppliers_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        clean_country,
        country_city_key,
        rating_scaled,
        city_filled,
        rating * rating_scaled AS rating_times_rating_scaled
    FROM cleaned_suppliers
    WHERE country != 'DE'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.clean_country,
        s.country_city_key,
        top1.product_brand
    FROM cleaned_suppliers_2 s
    JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country_city_key,
        product_brand,
        CONCAT(city, ' - ', product_brand) AS city_product_brand,
        COALESCE(product_brand, 'N/A') AS product_brand_filled
    FROM latest_match
    WHERE city = 'New York'
      AND city IS NOT NULL
),
stats AS (
    -- Multi-level aggregation
    SELECT
        product_brand,
        COUNT(CASE WHEN product_brand = 'Initech' THEN 1 END) AS initech_count,
        COUNT(*) FILTER (WHERE product_brand = 'Acme') AS acme_rows,
        GROUPING(product_brand) AS grouping_id
    FROM filtered
    GROUP BY ROLLUP (product_brand)
    HAVING COUNT(*) FILTER (WHERE product_brand = 'Acme') > 1
)
SELECT initech_count, grouping_id
FROM stats;
