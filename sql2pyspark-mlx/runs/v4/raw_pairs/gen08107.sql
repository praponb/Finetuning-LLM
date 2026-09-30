SELECT
    i.product_id,
    i.quantity,
    ss.sale_date,
    ADD_MONTHS(ss.sale_date, 3) AS sale_date_plus_months
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
