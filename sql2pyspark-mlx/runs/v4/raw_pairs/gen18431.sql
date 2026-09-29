select id, opened_date, store_name, region from stores where opened_date = (select max(opened_date) from stores)
