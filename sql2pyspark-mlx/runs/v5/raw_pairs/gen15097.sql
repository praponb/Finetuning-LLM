select s.country, case when s.rating >= 3.4 then 'high' when s.rating >= 1.4 then 'mid' else 'low' end as rating_bucket, count(*) as total_count
from products p
join suppliers s on s.id = p.supplier_id
group by s.country, case when s.rating >= 3.4 then 'high' when s.rating >= 1.4 then 'mid' else 'low' end;
