select i.product_id, p.launch_date, p.supplier_id, p.product_name from inventory i left join products p on i.product_id = p.id order by product_name, p.launch_date;
