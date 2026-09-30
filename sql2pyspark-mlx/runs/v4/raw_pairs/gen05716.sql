select id, store_name, concat_ws('-', region, store_name) as region_store_name_key from stores where length(store_name) >= 9
