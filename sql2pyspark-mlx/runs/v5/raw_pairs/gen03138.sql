WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(city, 1, 2) AS city_prefix,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-08-11'
      AND sqft < 13723
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id = 9
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_discount
    FROM eligible s
    JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        TRIM(region) AS clean_region,
        COALESCE(region, 'NONE') AS region_clean
    FROM with_latest
    WHERE opened_date >= TO_DATE('2023-08-13')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        region,
        opened_date,
        city_prefix,
        order_discount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY order_discount DESC NULLS LAST, id) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN order_discount >= 0.39 THEN 'HIGH' ELSE 'LOW' END AS order_discount_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    region,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(region)) AS num_regions,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest
GROUP BY 1, 2, 3
