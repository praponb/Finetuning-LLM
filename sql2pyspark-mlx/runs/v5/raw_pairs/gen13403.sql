SELECT id, sqft, DATE_TRUNC('year', opened_date) AS opened_date_period FROM stores WHERE YEAR(opened_date) >= 2024
