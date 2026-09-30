SELECT
    ss.id,
    ss.product_id,
    s.sqft,
    s.store_name,
    s.opened_date,
    ADD_MONTHS(ss.sale_date, 6) AS sale_date_plus_months
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
WHERE s.opened_date >= '2023-11-01'
ORDER BY store_name, product_id DESC;
