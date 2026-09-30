select p.category, case when p.cost >= 790 then 'high' when p.cost >= 120 then 'mid' else 'low' end as cost_bucket, count(distinct i.quantity) as unique_stores
from inventory i
inner join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
group by p.category, case when p.cost >= 790 then 'high' when p.cost >= 120 then 'mid' else 'low' end;
