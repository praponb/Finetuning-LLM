SELECT id, region, opened_date, store_name FROM stores WHERE region IN ('West', 'North', 'South') AND UPPER(TRIM(store_name)) = 'SIAM FLAGSHIP' ORDER BY opened_date DESC;
