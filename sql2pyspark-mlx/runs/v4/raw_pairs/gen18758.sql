select id, opened_date, region, store_name from stores where opened_date = (select min(opened_date) from stores)
