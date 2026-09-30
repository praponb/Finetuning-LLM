SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, MIN(i.quantity) AS min_quantity FROM inventory i FULL OUTER JOIN stores s ON i.store_id = s.id GROUP BY DATE_TRUNC('MONTH', s.opened_date)
