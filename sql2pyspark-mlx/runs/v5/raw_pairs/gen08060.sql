select i.id, i.quantity, i.store_id, ss.units from inventory i left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where ss.store_id = 4 order by id;
