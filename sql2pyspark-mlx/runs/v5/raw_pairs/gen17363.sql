select i.last_restock, p.launch_date, p.brand, s.store_name from inventory i left join products p on p.id = i.product_id inner join stores s on i.store_id = s.id order by last_restock desc;
