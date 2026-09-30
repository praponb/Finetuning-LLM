select p.brand, p.category, max(ss.units) - min(ss.units) units_range
from store_sales ss
inner join products p on ss.product_id = p.id
full outer join stores s on ss.store_id = s.id
where ss.product_id in (2, 7, 10)
group by p.brand, p.category
having max(ss.units) - min(ss.units) > 53
order by p.category;
