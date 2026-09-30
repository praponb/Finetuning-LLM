SELECT
    ss.store_id,
    ss.sale_date,
    s.opened_date,
    s.region,
    s.store_name,
    DATEDIFF(CURRENT_DATE(), ss.sale_date) AS days_since_sale_date
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE s.store_name LIKE '%Expres%'
ORDER BY s.opened_date;
