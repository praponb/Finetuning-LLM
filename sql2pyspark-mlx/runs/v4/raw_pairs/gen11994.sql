select s.city, s.country, p.category, p.product_name from suppliers s left join products p on p.supplier_id = s.id where s.country != 'JP'
