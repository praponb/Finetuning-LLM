WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        units * revenue AS revenue_per_unit,
        ROUND(units / revenue, 2) AS revenue_per_unit_2
    FROM store_sales
    WHERE sale_date > DATE '2023-09-03'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
        ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
    FROM cleaned
    WHERE sale_date >= DATE '2023-03-01'
      AND product_id IN (7, 9, 10)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY days_since_sale_date DESC, id ASC) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
latest_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue_per_unit_2 DESC, id ASC) AS rn
    FROM latest
    QUALIFY rn = 1
),
filtered_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        revenue_per_unit,
        revenue_per_unit_2,
        days_since_sale_date,
        sale_date_plus_months,
        rn,
        CASE WHEN revenue_per_unit_2 >= 83 THEN 'High' WHEN revenue_per_unit_2 >= 16 THEN 'Medium' ELSE 'Low' END AS revenue_per_unit_2_band
    FROM latest_store_sales
    WHERE sale_date >= DATE '2023-09-22'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    store_id,
    kpi,
    val
FROM (
    SELECT
        id,
        store_id,
        revenue_per_unit_2,
        revenue_per_unit,
        revenue
    FROM filtered_store_sales
) src
UNPIVOT (
    val FOR kpi IN (revenue_per_unit_2, revenue_per_unit AS REVENUE_PER_UNIT_VAL, revenue AS REVENUE_VAL)
)
