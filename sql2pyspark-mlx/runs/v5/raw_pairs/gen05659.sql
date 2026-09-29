SELECT
    id,
    store_id,
    revenue,
    sale_date,
    LEAST(units, revenue) AS least_units_revenue
FROM store_sales
WHERE revenue > 3100
