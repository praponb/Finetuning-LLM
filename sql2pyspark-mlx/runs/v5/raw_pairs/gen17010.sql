WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end,
        ADD_MONTHS(sale_date, 6) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > DATE '2023-02-17'
      AND revenue BETWEEN 2400 AND 3200
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.cost
    FROM base s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.launch_date < '2024-09-08'
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM with_dims
    EXCEPT
    SELECT product_id
    FROM store_sales
    WHERE product_id IN (2, 5, 9, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.product_id = k.product_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        product_id,
        sale_date,
        units,
        revenue,
        cost,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue
    FROM kept_rows
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN units >= 36 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
        CASE WHEN cost >= 800 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
        CASE WHEN corr_units_revenue >= 0.45 THEN 'LARGE' ELSE 'SMALL' END AS corr_units_revenue_tier,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        ROUND(AVG(revenue), 2) AS avg_revenue,
        APPROX_PERCENTILE(corr_units_revenue, 0.9) AS corr_units_revenue_p90
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN units >= 36 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cost >= 800 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN corr_units_revenue >= 0.45 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    units_tier,
    corr_units_revenue_tier,
    distinct_product_ids,
    avg_revenue,
    corr_units_revenue_p90
FROM grouped;
