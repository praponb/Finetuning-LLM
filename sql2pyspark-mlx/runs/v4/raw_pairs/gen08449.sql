SELECT
    id,
    store_id,
    LAST_DAY(sale_date) AS sale_date_month_end,
    CAST(revenue AS INT) AS revenue_int
FROM store_sales
