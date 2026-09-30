WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok
    FROM suppliers
    WHERE rating >= 4.0
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        is_bangkok,
        LENGTH(country) AS country_length,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM cleaned
    WHERE city IS NOT NULL
      AND city = 'New York'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        is_bangkok,
        country_length,
        resolved_city,
        SUM(is_bangkok) OVER w_ordered AS running_sum_is_bangkok,
        MAX(is_bangkok) OVER w_ordered AS running_max_is_bangkok,
        MIN(is_bangkok) OVER w_ordered AS running_min_is_bangkok
    FROM cleaned_suppliers
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.category,
        latest.brand
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.category, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(country_length, 1) AS country_length_rounded,
        country_length / NULLIF(running_sum_is_bangkok, 0) AS country_length_per_running_sum_is_bangkok,
        COALESCE(category, 'DIRECT') AS category_filled
    FROM with_latest
    WHERE category IS NOT NULL
      AND running_sum_is_bangkok IS NOT NULL
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    is_bangkok,
    country_length,
    resolved_city,
    running_sum_is_bangkok,
    running_max_is_bangkok
FROM filtered
ORDER BY rating DESC;
