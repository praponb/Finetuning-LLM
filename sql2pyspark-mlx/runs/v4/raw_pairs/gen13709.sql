SELECT id, units, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales WHERE NOT (store_id IN (1, 2, 4) AND product_id IN (3, 5)) ORDER BY id
