WITH filtered AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        LEAST(revenue, units) AS least_revenue_units,
        CAST(units AS DOUBLE) AS units_str
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-08'
)
SELECT
    a.id,
    a.product_id,
    a.revenue,
    d.product_name,
    d.category
FROM filtered a
LEFT JOIN products d
    ON a.product_id = d.id;
