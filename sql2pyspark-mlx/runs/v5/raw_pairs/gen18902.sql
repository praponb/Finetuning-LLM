select year(p.launch_date) as launch_year, min(oi.unit_price) as min_unit_price
from order_items oi
right join products p on oi.product_id = p.id
left join suppliers s on p.supplier_id = s.id
where s.country = 'SG'
group by year(p.launch_date)
having min_unit_price >= 330
order by launch_year desc, min_unit_price desc
