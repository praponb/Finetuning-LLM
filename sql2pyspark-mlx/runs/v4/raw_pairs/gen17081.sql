SELECT
    id,
    store_name,
    CAST(sqft AS STRING) AS sqft_num,
    DATE_ADD(opened_date, 2) AS opened_date_plus_2d
FROM stores
WHERE opened_date <= '2023-07-01';
