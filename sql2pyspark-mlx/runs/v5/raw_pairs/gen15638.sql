select case when s.rating >= 4.4 then 'high' when s.rating >= 2.4 then 'mid' else 'low' end as rating_bucket, count(case when p.brand = 'Initech' then 1 end) as initech_count, min(p.launch_date) as min_launch_date
from products p
inner join suppliers s on p.supplier_id = s.id
group by case when s.rating >= 4.4 then 'high' when s.rating >= 2.4 then 'mid' else 'low' end
order by rating_bucket, min_launch_date desc
limit 10;
