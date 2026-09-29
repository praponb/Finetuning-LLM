SELECT
    id,
    units,
    store_id,
    GREATEST(revenue, units) greatest_revenue_units
FROM store_sales
ORDER BY 3 DESC, id DESC
