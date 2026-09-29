select products.id, products.cost, products.category
from products
where products.id in (select product_id from inventory where quantity <> 219);
