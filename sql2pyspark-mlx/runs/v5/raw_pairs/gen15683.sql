WITH cleaned AS (
    SELECT
        *,
        CAST(quantity AS STRING) quantity_num,
        ROUND(quantity * 1.15, 2) quantity_adjusted
    FROM inventory
    WHERE last_restock BETWEEN '2024-03-01' AND '2024-05-21'
),
with_windows AS (
    SELECT
        *,
        MIN(quantity) OVER (PARTITION BY product_id) product_id_min_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) row_seq
    FROM cleaned
    WHERE quantity_adjusted >= 48
),
ranked AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_num,
        quantity_adjusted,
        product_id_min_quantity
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) = 1
),
enriched AS (
    SELECT
        a.*,
        s.sqft,
        s.city,
        s.region
    FROM ranked a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
valid AS (
    SELECT
        *,
        FLOOR(quantity) quantity_whole,
        COALESCE(city, 'N/A') city_filled
    FROM enriched
    WHERE last_restock > DATE '2023-08-17'
      AND city IS NOT DISTINCT FROM 'London'
)
SELECT
    store_id,
    product_id,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT_IF(region = 'Central') AS central_events,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    GROUPING(store_id) AS gid
FROM valid
GROUP BY GROUPING SETS ((store_id, product_id, DATE_TRUNC('MONTH', last_restock)), (store_id, product_id), (store_id), ())
ORDER BY singapore_count;
