WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        rating,
        CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND LENGTH(supplier_name) >= 8
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        rating,
        is_chiang_mai,
        resolved_city,
        CASE WHEN is_chiang_mai >= 71 THEN 'High' WHEN is_chiang_mai >= 22 THEN 'Medium' ELSE 'Low' END AS is_chiang_mai_band,
        TRIM(city) AS clean_city,
        COALESCE(city, 'OTHER') AS resolved_city_2
    FROM cleaned
    WHERE is_chiang_mai = 40
),
cleaned_suppliers_2 AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(rating AS INT) AS rating_int,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM cleaned_suppliers
    WHERE city IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(is_chiang_mai) OVER (PARTITION BY city) AS city_max_is_chiang_mai,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM cleaned_suppliers_2
    WINDOW w_hist AS (PARTITION BY city ORDER BY rating_int, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    city,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows
FROM scored
GROUP BY CUBE (CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END, city)
HAVING new_york_rows >= 1
ORDER BY new_york_rows;
