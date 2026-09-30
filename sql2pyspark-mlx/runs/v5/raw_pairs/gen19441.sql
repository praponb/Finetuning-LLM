select s.country, sum(i.quantity) total_quantity
from inventory i
inner join products p on i.product_id = p.id
inner join suppliers s on s.id = p.supplier_id
where s.rating > 3.6
group by s.country
order by s.country desc
limit 3;
