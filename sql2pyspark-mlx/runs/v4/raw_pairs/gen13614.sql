WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE YEAR(sale_date) = 2023
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM valid
    EXCEPT ALL
    SELECT store_id
    FROM store_sales
    WHERE product_id = 4
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.store_id = k.store_id
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        revenue_int,
        DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label
    FROM reconciled
    WHERE sale_date > DATE '2024-01-07'
      AND revenue > 1200
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
    AVG(revenue) AS avg_revenue,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    STDDEV_SAMP(revenue) AS revenue_stddev_2,
    MAX(revenue_int) AS max_revenue_int,
    GROUPING(store_id) AS gid
FROM valid_store_sales
GROUP BY GROUPING SETS ((store_id, CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END), (store_id), ())
ORDER BY store_id
