select s.id, s.supplier_name, s.city, s.rating from suppliers s where s.supplier_name like '%Cit%' and s.id in (select supplier_id from products) order by city
