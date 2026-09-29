select s.id, s.supplier_name, s.rating from suppliers s where s.country in ('US', 'SG', 'TH') and exists (select 1 from products p where p.supplier_id = s.id and p.brand != 'Initech');
