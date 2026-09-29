WITH filtered AS (
    SELECT
        *,
        CAST(quantity AS DOUBLE) quantity_num,
        DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_label
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-08-05')
),
enriched AS (
    SELECT
        src.*,
        d.city,
        d.store_name,
        d.opened_date
    FROM filtered src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_num, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
base AS (
    SELECT
        *,
        quantity * quantity_num quantity_times_quantity_num,
        ROUND(quantity * 1.07, 2) adjusted_quantity
    FROM ranked
    WHERE last_restock > DATE '2023-08-04'
      AND city IS NOT NULL
),
totals AS (
    SELECT
        CASE WHEN quantity >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
        product_id,
        APPROX_PERCENTILE(quantity_num, ARRAY(0.25, 0.5, 0.75)) AS quantity_num_quartiles,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        COUNT_IF(city = 'New York') AS new_york_events,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        SUM(adjusted_quantity) AS total_adjusted_quantity
    FROM base
    GROUP BY GROUPING SETS ((CASE WHEN quantity >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id), (CASE WHEN quantity >= 217 THEN 'PREMIUM' ELSE 'STANDARD' END))
    HAVING total_adjusted_quantity > 0
)
SELECT
    quantity_tier,
    city_list,
    new_york_events,
    distinct_store_ids
FROM totals
ORDER BY quantity_tier DESC;
