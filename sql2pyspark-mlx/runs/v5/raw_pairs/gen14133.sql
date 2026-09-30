select suppliers.id, suppliers.city, count(products.id) as product_count
from suppliers
left join products on suppliers.id = products.supplier_id
group by suppliers.id, suppliers.city;
