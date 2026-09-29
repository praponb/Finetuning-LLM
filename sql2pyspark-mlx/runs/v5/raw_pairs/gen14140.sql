SELECT
    id,
    revenue,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
    LAST_DAY(sale_date) AS sale_date_month_end
FROM store_sales
