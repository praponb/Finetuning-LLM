select p.category, p.price, p.brand, s.city, s.rating from products p join suppliers s on s.id = p.supplier_id where s.country <> 'SG' or s.supplier_name like '%Cit%';
