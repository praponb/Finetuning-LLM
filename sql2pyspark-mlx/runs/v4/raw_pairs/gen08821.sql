SELECT *, DATE_ADD(sale_date, 55) AS sale_date_plus_55d
FROM store_sales
ORDER BY store_id DESC, sale_date_plus_55d DESC;
