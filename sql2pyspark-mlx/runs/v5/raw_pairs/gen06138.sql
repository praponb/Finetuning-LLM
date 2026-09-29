SELECT
    YEAR(sale_date) sale_year,
    store_id,
    COUNT(*) total_count,
    AVG(units) avg_units
FROM store_sales
GROUP BY sale_year, store_id;
