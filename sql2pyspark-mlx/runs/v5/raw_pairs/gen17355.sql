SELECT id, sqft, ROW_NUMBER() OVER (ORDER BY sqft, id) AS row_num FROM stores WHERE YEAR(opened_date) >= 2025;
