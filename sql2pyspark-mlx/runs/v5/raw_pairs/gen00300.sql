select p.brand, min(ss.units) as min_units, count(distinct ss.units) as unique_stores
from store_sales ss
inner join products p on ss.product_id = p.id
where p.product_name not like 'C%'
group by p.brand
order by brand asc
