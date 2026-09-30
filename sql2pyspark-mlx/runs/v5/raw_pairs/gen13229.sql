SELECT
    id,
    opened_date,
    sqft,
    store_name,
    LAST_DAY(opened_date) AS opened_date_month_end
FROM stores
WHERE city IN ('Bangkok', 'New York', 'Tokyo')
