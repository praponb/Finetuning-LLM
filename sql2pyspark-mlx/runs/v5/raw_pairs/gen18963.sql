select s.city, count(p.id) as product_count from suppliers s join products p on p.supplier_id = s.id where s.rating between 2.2 and 2.8 group by s.city having count(p.id) >= 1
