SELECT id, city, opened_date, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS opened_date_row FROM stores
