select stores.id, stores.opened_date, stores.city from stores where stores.sqft > 9799 and stores.id in (select store_id from store_sales);
