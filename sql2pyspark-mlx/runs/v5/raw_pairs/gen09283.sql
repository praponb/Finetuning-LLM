select s.country, count(p.id) as product_count from suppliers s left join products p on p.supplier_id = s.id where s.rating > 4.1 group by s.country;
