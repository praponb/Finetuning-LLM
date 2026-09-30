SELECT id, opened_date, RANK() OVER (ORDER BY opened_date DESC) AS opened_date_rank FROM stores;
