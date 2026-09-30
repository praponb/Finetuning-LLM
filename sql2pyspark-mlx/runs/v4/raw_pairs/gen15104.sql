select s.country, round(avg(ss.units), 2) as avg_units
from store_sales ss
left join products p on p.id = ss.product_id
inner join suppliers s on s.id = p.supplier_id
where p.product_name not like '%Lapt%'
group by s.country
having round(avg(ss.units), 2) >= 45;
