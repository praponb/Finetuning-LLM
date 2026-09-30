select year(s.opened_date) as opened_year, min(i.quantity) as min_quantity from inventory i join stores s on i.store_id = s.id group by year(s.opened_date);
