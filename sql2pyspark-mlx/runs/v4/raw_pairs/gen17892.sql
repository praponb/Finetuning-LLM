WITH cleaned AS (
    SELECT *, GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE sale_date > DATE '2023-06-03'
),
filtered AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        GREATEST(revenue, units) AS greatest_revenue_units,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM cleaned
    WHERE sale_date > DATE '2024-01-06'
),
kept_keys AS (
    SELECT id, product_id
    FROM filtered
    INTERSECT
    SELECT id, product_id
    FROM store_sales
    WHERE units <= 11
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
cleaned_store_sales AS (
    SELECT
        id,
        sale_date,
        units,
        revenue,
        greatest_revenue_units,
        sale_date_month_end,
        LAST_DAY(sale_date_month_end) AS sale_date_month_end_month_end
    FROM reconciled
    WHERE units = 29
)
SELECT
    CASE WHEN revenue >= 3500 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    BIT_AND(units) AS units_bit_and,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    ROUND(AVG(units), 2) AS avg_units
FROM cleaned_store_sales
GROUP BY revenue_tier;
