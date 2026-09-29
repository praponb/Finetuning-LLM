SELECT
    id,
    sqft,
    store_name,
    ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
FROM stores;
