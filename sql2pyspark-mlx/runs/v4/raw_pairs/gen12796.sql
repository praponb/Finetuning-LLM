SELECT YEAR(s.opened_date) AS opened_year, MAX(ss.sale_date) AS last_sale_date FROM store_sales ss JOIN stores s ON s.id = ss.store_id GROUP BY YEAR(s.opened_date)
