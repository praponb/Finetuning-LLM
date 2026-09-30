select s.city, count(i.quantity) quantity_count, count(distinct i.quantity) unique_products
from inventory i
inner join products p on p.id = i.product_id
left join stores s on i.store_id = s.id
where s.region <> 'North'
group by s.city
order by quantity_count;
