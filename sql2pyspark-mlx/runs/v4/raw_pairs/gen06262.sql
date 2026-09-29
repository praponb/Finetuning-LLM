select s.id, s.supplier_name, count(p.id) as product_count from suppliers s left join products p on p.supplier_id = s.id group by s.id, s.supplier_name order by 3 asc;
