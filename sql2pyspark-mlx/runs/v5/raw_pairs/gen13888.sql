select s.id, s.city, count(p.id) as product_count from suppliers s inner join products p on s.id = p.supplier_id group by s.id, s.city order by product_count desc;
