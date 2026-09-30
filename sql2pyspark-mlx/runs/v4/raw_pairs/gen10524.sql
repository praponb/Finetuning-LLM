select p.supplier_id, round(avg(ss.revenue), 2) as avg_revenue
from store_sales ss
left outer join products p on p.id = ss.product_id
left outer join stores s on ss.store_id = s.id
where p.stock_qty = 307
group by p.supplier_id
order by p.supplier_id
limit 20;
