SELECT
    revenue,
    store_id,
    units,
    LEAST(revenue, units) AS least_revenue_units,
    DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales;
