select p.category, case when p.cost >= 710 then 'high' when p.cost >= 160 then 'mid' else 'low' end as cost_bucket, max(ss.revenue) - min(ss.revenue) as revenue_range
from store_sales ss
left outer join products p on p.id = ss.product_id
where ss.units between 21 and 30
group by p.category, case when p.cost >= 710 then 'high' when p.cost >= 160 then 'mid' else 'low' end
