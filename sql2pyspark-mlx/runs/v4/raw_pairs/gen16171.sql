select products.id, products.brand, products.supplier_id
from products
where products.supplier_id not in (select id from suppliers where city in ('Tokyo', 'Berlin'))
