select suppliers.id, suppliers.rating from suppliers where exists (select 1 from products where products.supplier_id = suppliers.id and products.brand = 'Initech')
