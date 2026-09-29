select p.brand, max(ss.units) - min(ss.units) as units_range
from store_sales ss
inner join products p on ss.product_id = p.id
inner join stores s on ss.store_id = s.id
group by p.brand
order by units_range desc, brand desc
limit 20
