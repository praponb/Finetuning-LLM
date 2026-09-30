select rating_bucket, new_york_count, num_suppliers
from (
    select case when rating >= 3.8 then 'high' when rating >= 1.5 then 'mid' else 'low' end as rating_bucket, sum(case when city = 'New York' then 1 else 0 end) as new_york_count, count(*) as num_suppliers
    from suppliers
    group by case when rating >= 3.8 then 'high' when rating >= 1.5 then 'mid' else 'low' end
) as sub
where new_york_count >= 80
order by rating_bucket
limit 5;
