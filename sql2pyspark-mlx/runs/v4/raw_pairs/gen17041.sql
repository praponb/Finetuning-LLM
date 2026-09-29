WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT_WS(',', supplier_name, country) AS supplier_name_country_key,
        COALESCE(city, 'NONE') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
      AND country IN ('JP', 'UK')
),
base AS (
    SELECT
        id,
        city,
        supplier_name_country_key,
        city_clean,
        CASE WHEN rating >= 4.3 THEN 'High' WHEN rating >= 2.1 THEN 'Medium' ELSE 'Low' END AS rating_band,
        UPPER(city) AS city_upper
    FROM cleaned
    WHERE city IS NOT NULL
),
latest_match AS (
    SELECT s.*, top1.category
    FROM base s
    JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
daily AS (
    SELECT
        city,
        category,
        SIZE(COLLECT_SET(category)) AS num_categories,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        COUNT(DISTINCT category) AS distinct_categories,
        COUNT(DISTINCT id) AS distinct_ids
    FROM latest_match
    GROUP BY city, category
)
SELECT
    city,
    category,
    num_categories,
    distinct_categories,
    distinct_ids,
    CASE WHEN city RLIKE '^(London|Singapore)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'PRIORITY' END AS city_zone
FROM daily
ORDER BY city_zone DESC
