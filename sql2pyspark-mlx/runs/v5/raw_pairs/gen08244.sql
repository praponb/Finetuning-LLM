SELECT * FROM stores WHERE MONTH(opened_date) IN (6, 7, 11) AND opened_date > '2023-03-30' AND UPPER(TRIM(store_name)) = 'SIAM FLAGSHIP' ORDER BY city ASC
