select p.brand, min(oi.unit_price) as min_unit_price
from order_items oi
inner join products p on oi.product_id = p.id
where p.cost between 370 and 380
group by p.brand
having min(oi.unit_price) >= 310
