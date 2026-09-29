select s.city, min(p.price) as min_price, concat_ws(', ', sort_array(collect_set(p.category))) as category_values from products p inner join suppliers s on p.supplier_id = s.id group by s.city
