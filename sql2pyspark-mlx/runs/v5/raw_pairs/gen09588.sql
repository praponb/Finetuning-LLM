select s.id, s.opened_date, s.region, s.sqft from stores s where s.id in (select store_id from store_sales where units between 19 and 30);
