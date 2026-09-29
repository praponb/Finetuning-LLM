select p.category, p.supplier_id, round(stddev(ss.units), 2) as stddev_units
from store_sales ss
left join products p on ss.product_id = p.id
group by p.category, p.supplier_id
order by p.supplier_id, category
limit 10;
